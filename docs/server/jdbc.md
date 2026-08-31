# ConfluxQuery JDBC Driver

The ConfluxQuery JDBC Driver is the supported Java entry point to
ConfluxQuery Gateway. It is a Type 4 driver: Java applications connect directly
to the Gateway's Arrow Flight SQL listener, and the Gateway routes each
connection to one configured Trino, Databricks SQL, or Snowflake target.

The driver owns the stable `jdbc:qcli://` contract and delegates Flight SQL
transport and Arrow-to-JDBC conversion to Apache Arrow JDBC 19.0.0. This keeps
the ConfluxQuery layer small while insulating applications from transport
headers and upstream URL details.

## Install

Java 17 and 21 are tested. Once the artifact is available from Maven Central:

```xml
<dependency>
  <groupId>in.confluxdata</groupId>
  <artifactId>confluxquery-jdbc</artifactId>
  <version>0.1.0</version>
</dependency>
```

Release pages also contain `confluxquery-jdbc-<version>-all.jar`, a standalone
JAR with its runtime dependencies. The ordinary Maven artifact is preferable
for applications that already manage dependencies. The driver registers
through Java's service-provider mechanism; `Class.forName` is not required.

Apache Arrow 19 on Java 17+ requires this JVM option:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

## Connect and select a target

The URL has exactly one target path segment:

```text
jdbc:qcli://gateway-host:32010/target-name
```

Port `32010` is used when the port is omitted. Target names are section headers
from the Gateway configuration. They are sent as the `qcli-target` Flight
metadata header, so a client cannot silently fall back to a different target.

```java
Properties properties = new Properties();
properties.setProperty("token", System.getenv("QCLI_TOKEN"));
properties.setProperty("catalog", "hive");
properties.setProperty("schema", "sales");

try (Connection connection = DriverManager.getConnection(
        "jdbc:qcli://gateway.example.com:32010/trino-prod", properties);
     PreparedStatement statement = connection.prepareStatement(
        "select * from orders where orderkey = ?")) {
  statement.setLong(1, 42);
  try (ResultSet rows = statement.executeQuery()) {
    while (rows.next()) System.out.println(rows.getLong("orderkey"));
  }
}
```

## Properties

| Property | Default | Purpose |
|---|---|---|
| `token` | required | Opaque Gateway API key or bearer token. |
| `jwt` | — | OIDC JWT alias for `token`; do not set both. |
| `catalog` | — | Initial catalog/database, when supported by the target. |
| `schema` | — | Initial schema, when supported by the target. |
| `tls` | `true` | Encrypt the Flight SQL connection. |
| `certificateVerification` | `true` | Verify the Gateway certificate. Keep enabled in production. |
| `trustStore` | — | Java trust-store path. |
| `trustStorePassword` | — | Trust-store password. |
| `useSystemTrustStore` | — | Use the JVM/system certificate trust. |
| `tlsRootCerts` | — | PEM root-certificate path. |
| `clientCertificate` | — | PEM client certificate for mTLS. |
| `clientKey` | — | PEM client private-key path for mTLS. |
| `connectTimeoutMs` | `10000` | Connection timeout in milliseconds. |
| `retainCookies` | `true` | Preserve Gateway session cookies. |
| `retainAuth` | `true` | Preserve authorization across calls. |

Non-secret transport settings may appear after `?` in the URL, for example
`?tls=false&connectTimeoutMs=2500`. Credentials, passwords, duplicate keys,
unknown URL keys, user information, and URL fragments are rejected. Supply
secrets through `Properties`, a data-source secret provider, or your framework's
credential facility. `DriverPropertyInfo` deliberately returns no secret
values, and `DatabaseMetaData.getURL()` returns a credential-free branded URL.

For production TLS, start the Gateway with `--flight-tls-cert` and
`--flight-tls-key`, leave `tls=true`, and configure a trusted CA. Add
`clientCertificate` and `clientKey` when the Gateway requires mTLS. OIDC access
tokens use `jwt`; token acquisition and refresh remain the application's or
identity library's responsibility.

## Frameworks and pools

HikariCP and Spring JDBC are exercised by release conformance:

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:qcli://gateway.example.com:32010/trino-prod");
config.setDriverClassName("in.confluxdata.query.jdbc.ConfluxQueryDriver");
config.addDataSourceProperty("token", System.getenv("QCLI_TOKEN"));

try (HikariDataSource pool = new HikariDataSource(config)) {
  JdbcTemplate jdbc = new JdbcTemplate(pool);
  List<Map<String, Object>> rows = jdbc.queryForList("select * from nation");
}
```

For database tools, add the standalone JAR as a custom JDBC driver, set the
class to `in.confluxdata.query.jdbc.ConfluxQueryDriver`, and enter a
`jdbc:qcli://` URL. A tool is supported only if its exact version appears in
the [compatibility matrix](../connectivity-compatibility.md); generic JAR
loading does not constitute certification.

## Sessions and lifecycle

One JDBC connection owns one logical Gateway session. Cookies preserve the
server session across Flight calls. Catalog and schema setters use Flight SQL
session properties where supported. Closing the connection releases the
logical session; always close result sets, statements, and connections or use
try-with-resources. Pools must validate connections and have a finite maximum
lifetime so abandoned server state is eventually reclaimed.

`Statement.cancel()` maps to Flight SQL cancellation. JDBC query timeouts rely
on the underlying Flight deadline behavior. A cancel request is cooperative:
the selected engine may take time to acknowledge it. Updates return the count
reported by the engine/Gateway contract.

## Errors and diagnostics

Invalid URLs and connection setup failures are non-transient connection
errors. SQL and engine errors are exposed as `SQLException` instances by the
Arrow layer. Unsupported JDBC APIs, including callable statements, fail with
`SQLFeatureNotSupportedException`; they do not silently emulate behavior.
Gateway audit events contain the principal, target, session, and query ID.
Never enable client logging that prints connection properties in production.

## Build and verify locally

```text
mvn -B -f jdbc/pom.xml clean verify
mvn -B -f jdbc/pom.xml install
QCLI_FLIGHT_TOKEN=<key> MAVEN_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED \
  mvn -B -f conformance/m25/java/pom.xml compile exec:java \
  -Dexec.mainClass=in.confluxdata.query.jdbc.ConformanceProfile
```

The Maven build produces the thin JAR, standalone `-all.jar`, source and
Javadoc JARs, and a CycloneDX SBOM. Stable publication is guarded by repository
configuration and requires Maven Central credentials and a GPG signing key.

## Support boundary

The driver reports `jdbcCompliant() == false`. That is intentional: M25
supports the API surface named in the matrix, not every optional JDBC method.
Safe prepare-time schemas are available only where the selected engine adapter
can describe a query without executing it. ConfluxQuery never runs a query at
prepare time or invents a schema to create the appearance of compatibility.

