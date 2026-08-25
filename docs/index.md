<div class="hero" markdown>

# One query experience. Every cloud warehouse.

qcli is a Rust query client and gateway for Trino, Databricks SQL, and
Snowflake. Use it interactively in a terminal, automate exact machine output,
or expose governed HTTP and Arrow Flight SQL connectivity to an organization.

[Start with the CLI](getting-started/cli-quickstart.md){ .md-button .md-button--primary }
[Deploy the gateway](getting-started/server-quickstart.md){ .md-button }

</div>

!!! info "Release boundary"
    HTTP, native Flight SQL, Python/Go/Java/Rust ADBC profiles, and the upstream
    Arrow Flight SQL JDBC integration are released surfaces. ODBC remains
    experimental; the branded qcli JDBC driver is planned for M25.

<div class="grid cards" markdown>

-   :material-console: **A capable terminal**

    Discover targets, browse metadata, change context, stream queries, cancel
    work, and emit exact CSV, TSV, JSON, JSONL, table, or vertical output.

-   :material-server-network: **A reusable query gateway**

    One shared query runtime powers HTTP and Flight SQL with sessions,
    prepared statements, Arrow results, authentication, quotas, and audit.

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
| Install and run a first query | [CLI quickstart](getting-started/cli-quickstart.md) |
| Configure Trino, Databricks, or Snowflake | [Engine setup](guides/engines.md) |
| Start HTTP and Swagger | [Gateway quickstart](getting-started/server-quickstart.md) |
| Connect an application | [Client ecosystem](server/clients.md) |
| Understand the design | [Architecture](concepts/architecture.md) |
| Deploy and operate production | [Operations](operations.md) |
| Diagnose a failure | [Troubleshooting](guides/troubleshooting.md) |
