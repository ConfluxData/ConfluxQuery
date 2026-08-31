# Milestone 25 notes — ConfluxQuery JDBC Driver

## Outcome

M25 delivers a separately versioned Type 4 Java driver for ConfluxQuery
Gateway. Applications use `jdbc:qcli://host:port/target`; the driver translates
that stable contract into Apache Arrow Flight SQL JDBC calls and injects the
selected target without exposing transport-specific headers.

## Delivered

- Maven coordinates `in.confluxdata:confluxquery-jdbc:0.1.0`, Java service
  registration, Java 17 baseline, and Java 17/21 CI.
- Thin Maven JAR plus self-contained `-all.jar`, source/Javadoc artifacts,
  CycloneDX JSON SBOM, deterministic build timestamp, GPG/Central release
  profile, GitHub checksums, Sigstore signing, and provenance attestation.
- Credential-free `jdbc:qcli://` URLs with exact target routing, default port
  32010, TLS/mTLS/trust settings, bearer `token` and OIDC `jwt` alias, initial
  catalog/schema properties, connection deadlines, and cookie retention.
- Rejection of URL credentials, unknown and duplicate URL properties, user
  info, ambiguous targets, and fragments. Secret values are redacted from
  property discovery and database metadata.
- Branded `DatabaseMetaData` identity while delegating standard connection,
  statement, prepared statement, result set, metadata, update, cancellation,
  and lifecycle behavior to the pinned Arrow JDBC implementation.
- A packaged conformance application covering DriverManager/service discovery,
  statements/results, prepare-time parameter/result metadata, typed binding,
  updates, cancellation, unsupported callable statements, cleanup, HikariCP,
  and Spring JDBC.
- A protected live profile for ordinary queries and pooling against configured
  Trino, Databricks SQL, and Snowflake targets.
- Flight session creation now accepts the authenticated `qcli-target` metadata
  header when JDBC sends catalog/schema session options before a cookie exists;
  this enables initial catalog/schema without executing a bootstrap query.
- User documentation for installation, URLs, all public properties, TLS/mTLS,
  OIDC, pools/frameworks, tools, sessions, diagnostics, local validation, and
  the supported/unsupported boundary.

## Verification evidence

On 2026-08-26:

- `mvn -B -f jdbc/pom.xml clean verify` passed 7 unit tests and built thin,
  standalone, source, Javadoc, and SBOM artifacts without Javadoc warnings.
- The M25 profile passed against the real local ConfluxQuery Gateway Flight SQL
  listener using the demo target, including catalog/schema state, SQL metadata,
  cancellation, and pool/framework checks: `confluxquery-jdbc: PASS`.
- CI now repeats the driver build on Temurin 17 and 21, checks the standalone
  JAR hash across clean builds, and runs the branded profile against the
  packaged-equivalent Gateway.
- Stable release automation stages the Java artifacts alongside native qcli
  packages. Maven Central publication is opt-in through the protected
  `maven-central` environment.

## Compatibility and limitations

- Apache Arrow Flight SQL JDBC 19.0.0 is the pinned transport implementation.
  Java needs `--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow memory.
- The extended JDBC suite is certified for the deterministic demo adapter.
  Protected live Trino, Databricks SQL, and Snowflake jobs certify connection,
  branded metadata, ordinary statements/results, HikariCP, and Spring JDBC.
- Live-engine prepare metadata is not claimed. The current warehouse adapters
  cannot safely describe arbitrary query output without execution. ConfluxQuery
  does not execute during prepare or fabricate a result schema.
- Callable statements and other unnamed JDBC optional features are unsupported
  and must return `SQLFeatureNotSupportedException`. The driver deliberately
  reports `jdbcCompliant() == false`.
- Generic custom-driver loading is possible in Java database tools, but no
  specific desktop tool/version is certified in M25.
- OIDC token acquisition and refresh are caller responsibilities; the driver
  transports an already-issued bearer token.

## Release prerequisites

Maven Central publication requires the repository variable
`ENABLE_MAVEN_CENTRAL_PUBLISH=true`, Central username/password secrets, a GPG
private key and passphrase, and approval of the `maven-central` environment.
Cross-engine claims require a successful protected live-connectivity run with
the engine versions captured in that release's evidence.
