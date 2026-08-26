<div class="hero" markdown>

# ConfluxQuery

## Query anywhere. Govern access once.

ConfluxQuery is an open-source query toolset by
[ConfluxData](https://confluxdata.in/). Use ConfluxQuery CLI to query Trino,
Databricks SQL, and Snowflake from the terminal, or deploy ConfluxQuery Gateway
to connect applications through HTTP, Arrow Flight SQL, ADBC, and JDBC.

ConfluxQuery is distributed as the `qcli` command.

[Start with the CLI](getting-started/cli-quickstart.md){ .md-button .md-button--primary }
[Deploy the gateway](getting-started/server-quickstart.md){ .md-button }

</div>

!!! info "Release boundary"
    HTTP, native Flight SQL, named ADBC profiles, and the branded ConfluxQuery
    JDBC Driver are released surfaces. ODBC remains experimental. Consult the
    compatibility matrix before choosing a client/engine combination.

<div class="grid cards" markdown>

-   :material-console: **ConfluxQuery CLI**

    One consistent interactive and automated query experience across Trino,
    Databricks SQL, and Snowflake.

-   :material-server-network: **ConfluxQuery Gateway**

    A governed, Arrow-native query access layer for applications and data
    tools, powered by the same query runtime as the CLI.

-   :material-shield-lock: **Designed for enterprise boundaries**

    API keys, JWT/OIDC, mTLS, target ACLs, principal ownership, TLS proxy
    enforcement, bounded retention, and cluster-safe fencing are built in.

-   :material-source-branch: **Extensible by construction**

    Engine adapters, credential providers, protocol front ends, coordination,
    and result storage meet at explicit Rust interfaces rather than one giant
    command implementation.

</div>

## Choose your path

| Goal | Start here |
|---|---|
| Install and run a first query | [ConfluxQuery CLI quickstart](getting-started/cli-quickstart.md) |
| Configure Trino, Databricks, or Snowflake | [Engine setup](guides/engines.md) |
| Start HTTP and Swagger | [ConfluxQuery Gateway quickstart](getting-started/server-quickstart.md) |
| Connect an application | [Client ecosystem](server/clients.md) |
| Understand the design | [Architecture](concepts/architecture.md) |
| Deploy and operate production | [Operations](operations.md) |
| Diagnose a failure | [Troubleshooting](guides/troubleshooting.md) |
