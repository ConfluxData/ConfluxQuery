# Changelog

All notable qcli changes are documented here. qcli follows semantic versioning.

## [Unreleased]

### Added

- Cross-platform CI and tag-driven GitHub release automation.
- Signed archives, checksums, SBOM generation, and provenance attestations.
- Guarded crates.io and Homebrew publication workflows.
- `qcli --version`, shell completions, and a manual page.
- A non-root OCI gateway image plus Kubernetes and systemd deployment assets.
- Packaged HTTP, native Flight SQL, ADBC, and Arrow Flight SQL JDBC release
  profiles with explicit connectivity support boundaries.
- Liveness/readiness endpoints and environment-backed clustered deployment
  settings.
- Operator guidance for TLS, identity, scaling, upgrades, rollback, and
  incidents.
- A branded, searchable MkDocs product portal covering product rationale,
  architecture, every CLI/server surface, ecosystem examples, and how-to
  workflows, with strict CI builds and one-command GitHub Pages deployment.
- ConfluxQuery umbrella branding by ConfluxData, with ConfluxQuery CLI and
  ConfluxQuery Gateway as the two offerings while preserving `qcli` technical
  contracts, plus an authoritative terminology directive and CI enforcement.
- A branded Type 4 ConfluxQuery JDBC Driver with `jdbc:qcli://` target routing,
  secure bearer/TLS properties, standalone and Maven artifacts, Java 17/21
  gates, HikariCP/Spring conformance, SBOM generation, and guarded publication.

## [0.1.0] - Unreleased

- Initial Trino, Databricks SQL, and Snowflake CLI.
- Interactive and batch query execution with reusable session and query core.
- Authenticated production HTTP API with OpenAPI and Swagger UI.
