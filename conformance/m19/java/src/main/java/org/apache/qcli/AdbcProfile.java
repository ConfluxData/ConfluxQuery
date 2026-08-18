package org.apache.qcli;

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnectionProperties;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

public final class AdbcProfile {
  private AdbcProfile() {}

  public static void main(String[] args) throws Exception {
    String uri = System.getenv().getOrDefault("QCLI_FLIGHT_URI", "grpc://127.0.0.1:32010");
    String target = System.getenv().getOrDefault("QCLI_FLIGHT_TARGET", "demo");
    String query = System.getenv().getOrDefault("QCLI_FLIGHT_QUERY", "select * from sample");
    long expectedRows = Long.parseLong(System.getenv().getOrDefault("QCLI_FLIGHT_EXPECTED_ROWS", "2"));
    try (RootAllocator allocator = new RootAllocator()) {
      Map<String, Object> options = new HashMap<>();
      AdbcDriver.PARAM_URI.set(options, uri);
      options.put(
          FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "authorization",
          "Bearer " + required("QCLI_FLIGHT_TOKEN"));
      options.put(
          FlightSqlConnectionProperties.RPC_CALL_HEADER_PREFIX + "qcli-target", target);
      try (AdbcDatabase database = new FlightSqlDriver(allocator).open(options);
          AdbcConnection connection = database.connect()) {
        try (AdbcStatement statement = connection.createStatement()) {
          statement.setSqlQuery(query);
          try (AdbcStatement.QueryResult result = statement.executeQuery()) {
            check(countRows(result.getReader()) == expectedRows, "unexpected query row count");
          }
        }
        try (ArrowReader objects =
            connection.getObjects(
                AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)) {
          check(countRows(objects) > 0, "object metadata is empty");
        }
      }
    }
    System.out.println("java-adbc: PASS");
  }

  private static long countRows(ArrowReader reader) throws Exception {
    long rows = 0;
    while (reader.loadNextBatch()) rows += reader.getVectorSchemaRoot().getRowCount();
    return rows;
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
