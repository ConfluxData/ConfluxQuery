package org.apache.qcli;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public final class JdbcProfile {
  private JdbcProfile() {}

  public static void main(String[] args) throws Exception {
    String host = System.getenv().getOrDefault("QCLI_FLIGHT_HOST", "127.0.0.1");
    String port = System.getenv().getOrDefault("QCLI_FLIGHT_PORT", "32010");
    String token = required("QCLI_FLIGHT_TOKEN");
    String target = System.getenv().getOrDefault("QCLI_FLIGHT_TARGET", "demo");
    String uri = "jdbc:arrow-flight-sql://" + host + ":" + port + "/?useEncryption=false";
    Properties properties = new Properties();
    properties.setProperty("token", token);
    properties.setProperty("qcli-target", target);
    properties.setProperty("connectTimeout", "10000");

    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("select * from sample")) {
        int rows = 0;
        while (result.next()) rows++;
        check(rows == 2, "expected two query rows");
        check(result.getMetaData().getColumnCount() > 0, "missing result metadata");
      }

      DatabaseMetaData metadata = connection.getMetaData();
      try (ResultSet catalogs = metadata.getCatalogs()) {
        check(catalogs.next(), "catalog metadata is empty");
      }
      try (ResultSet tables = metadata.getTables(null, "%", "%", null)) {
        check(tables.next(), "table metadata is empty");
      }

      try (PreparedStatement prepared = connection.prepareStatement("select ?")) {
        prepared.setString(1, "typed-value");
        try (ResultSet result = prepared.executeQuery()) {
          check(result.next(), "prepared query returned no rows");
          check("typed-value".equals(result.getString(1)), "prepared value changed");
        }
      }

      try (Statement cancellable = connection.createStatement()) {
        var executor = Executors.newSingleThreadExecutor();
        try {
          var running =
              executor.submit(
                  () -> {
                    try (ResultSet result = cancellable.executeQuery("wait-for-cancel")) {
                      return result.next();
                    }
                  });
          Thread.sleep(100);
          cancellable.cancel();
          try {
            running.get();
            throw new AssertionError("cancelled JDBC query completed successfully");
          } catch (ExecutionException expected) {
            check(expected.getCause() instanceof java.sql.SQLException, "cancel was not a SQL error");
          }
        } finally {
          executor.shutdownNow();
        }
      }
    }
    System.out.println("arrow-jdbc: PASS");
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
