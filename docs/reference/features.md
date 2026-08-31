# Feature status

| Area | Feature | Status |
|---|---|---|
| CLI | Interactive and batch queries | Supported |
| CLI | Target switching and metadata navigation | Supported |
| CLI | Table, vertical, CSV, TSV, JSON, JSONL | Supported |
| Engines | Trino, Databricks SQL, Snowflake | Supported within adapter capabilities |
| HTTP | Sessions, stateless queries, status, paging, SSE, cancel | Supported |
| HTTP | OpenAPI and Swagger UI | Supported |
| Flight SQL | Discovery, sessions, queries, metadata | Supported |
| Flight SQL | Prepared statements, parameters, updates | Supported |
| Flight SQL | Ingestion and large transfer | Supported |
| Identity | API keys, OIDC/JWT, Flight mTLS | Supported |
| Operations | PostgreSQL/object-store cluster mode | Supported within documented topology |
| Clients | Python/Go/Java/Rust ADBC profiles | Supported named versions |
| Clients | Upstream Arrow Flight SQL JDBC | Supported integration profile |
| Clients | ConfluxQuery JDBC Driver | Supported named surface |
| Clients | ODBC and BI | Experimental |
| SQL | Dialect-aware read-only transpilation | Next — M26 |
| Intelligence | Query passport, eligibility, and plan analysis | Planned M27–M28 |
| Governance | Intelligent routing, cost, and workload management | Planned M29 |
| Agents | Governed MCP connectivity | Planned post-M26 |
| Migration | Cross-engine semantic validation | Planned post-M26 |
| Lakehouse | Metadata and pruning intelligence | Planned post-M26 |
| Quality | Data contracts and result validation | Planned post-M26 |
| Extensions | Public policy and plugin SDK | Planned post-M26 |

The authoritative version/client matrix is
[connectivity compatibility](../connectivity-compatibility.md). Feature ideas
that have not passed release gates remain in the [roadmap](../features-roadmap.md).
