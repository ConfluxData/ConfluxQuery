package in.confluxdata.query.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.springframework.jdbc.core.JdbcTemplate;

/** Packaged ConfluxQuery JDBC compatibility profile. */
public final class ConformanceProfile {
  private ConformanceProfile() {}

  public static void main(String[] args) throws Exception {
    String host = System.getenv().getOrDefault("QCLI_FLIGHT_HOST", "127.0.0.1");
    String port = System.getenv().getOrDefault("QCLI_FLIGHT_PORT", "32010");
    String target = System.getenv().getOrDefault("QCLI_FLIGHT_TARGET", "demo");
    String url = "jdbc:qcli://" + host + ":" + port + "/" + target + "?tls=false";
    Properties properties = new Properties();
    properties.setProperty("token", required("QCLI_FLIGHT_TOKEN"));
    properties.setProperty("connectTimeoutMs", "10000");
    String query = System.getenv().getOrDefault("QCLI_FLIGHT_QUERY", "select * from sample");
    int expectedRows = Integer.parseInt(System.getenv().getOrDefault("QCLI_FLIGHT_EXPECTED_ROWS", "2"));
    boolean extended = Boolean.parseBoolean(System.getenv().getOrDefault("QCLI_JDBC_EXTENDED", "true"));

    driverManagerProfile(url, properties, query, expectedRows, extended);
    hikariAndSpringProfile(url, properties, query, expectedRows);
    System.out.println("confluxquery-jdbc: PASS");
  }

  private static void driverManagerProfile(
      String url, Properties properties, String query, int expectedRows, boolean extended)
      throws Exception {
    Properties connectionProperties = new Properties();
    connectionProperties.putAll(properties);
    if (extended) {
      connectionProperties.setProperty("catalog", "analytics");
      connectionProperties.setProperty("schema", "public");
    }
    Connection connection = DriverManager.getConnection(url, connectionProperties);
    check(connection.isValid(2), "connection is not valid");
    DatabaseMetaData metadata = connection.getMetaData();
    check("ConfluxQuery JDBC Driver".equals(metadata.getDriverName()), "driver identity is not branded");
    check(metadata.getURL().equals(url.substring(0, url.indexOf('?'))), "public JDBC URL changed");
    try (ResultSet catalogs = metadata.getCatalogs()) {
      check(catalogs.next(), "catalog metadata is empty");
    }
    try (ResultSet tables = metadata.getTables(null, "%", "%", null)) {
      check(tables.next(), "table metadata is empty");
    }
    if (extended) {
      check("analytics".equals(connection.getCatalog()), "initial catalog was not retained");
      check("public".equals(connection.getSchema()), "initial schema was not retained");
    }

    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {
      int rows = 0;
      while (result.next()) rows++;
      check(rows == expectedRows, "unexpected row count");
      check(result.getMetaData().getColumnCount() > 0, "missing result metadata");
    }

    if (extended) try (PreparedStatement prepared = connection.prepareStatement("select ?")) {
      check(prepared.getParameterMetaData().getParameterCount() == 1, "missing parameter metadata");
      check(prepared.getMetaData() != null, "missing prepare-time result metadata");
      prepared.setString(1, "typed-value");
      try (ResultSet result = prepared.executeQuery()) {
        check(result.next(), "prepared query returned no row");
        check("typed-value".equals(result.getString(1)), "prepared value changed");
      }
    }

    if (extended) try (Statement update = connection.createStatement()) {
      check(update.executeUpdate("update demo set value = 'x'") == 1, "wrong update count");
    }

    if (extended) try (Statement cancellable = connection.createStatement()) {
      var executor = Executors.newSingleThreadExecutor();
      try {
        var running = executor.submit(() -> {
          try (ResultSet result = cancellable.executeQuery("wait-for-cancel")) {
            return result.next();
          }
        });
        Thread.sleep(100);
        cancellable.cancel();
        try {
          running.get();
          throw new AssertionError("cancelled query completed successfully");
        } catch (ExecutionException expected) {
          check(expected.getCause() instanceof SQLException, "cancel did not produce SQLException");
        }
      } finally {
        executor.shutdownNow();
      }
    }

    if (extended) try {
      connection.prepareCall("call unsupported()");
      throw new AssertionError("unsupported CallableStatement silently succeeded");
    } catch (SQLFeatureNotSupportedException expected) {
      // Correct JDBC capability response.
    }

    Statement closedStatement = connection.createStatement();
    closedStatement.close();
    check(closedStatement.isClosed(), "statement did not close");
    connection.close();
    check(connection.isClosed(), "connection did not close");
  }

  private static void hikariAndSpringProfile(
      String url, Properties properties, String query, int expectedRows) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setDriverClassName("in.confluxdata.query.jdbc.ConfluxQueryDriver");
    properties.forEach((name, value) -> config.addDataSourceProperty((String) name, value));
    config.setMaximumPoolSize(2);
    config.setConnectionTimeout(10_000);
    try (HikariDataSource dataSource = new HikariDataSource(config)) {
      JdbcTemplate template = new JdbcTemplate(dataSource);
      int rows = template.queryForList(query).size();
      check(rows == expectedRows, "Spring JdbcTemplate did not preserve the result set");
    }
  }

  private static String required(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) throw new IllegalStateException(name + " is required");
    return value;
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
