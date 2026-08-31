package in.confluxdata.query.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLNonTransientConnectionException;
import java.util.Arrays;
import java.util.Properties;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

final class ConfluxQueryDriverTest {
  private final ConfluxQueryDriver driver = new ConfluxQueryDriver();

  @Test
  void acceptsOnlyConfluxQueryUrls() {
    assertTrue(driver.acceptsURL("jdbc:qcli://localhost:32010/demo"));
    assertFalse(driver.acceptsURL("jdbc:arrow-flight-sql://localhost:32010/"));
    assertFalse(driver.acceptsURL(null));
  }

  @Test
  void translatesTargetTlsAndSessionProperties() throws Exception {
    Properties info = new Properties();
    info.setProperty("token", "secret-value");
    info.setProperty("catalog", "hive");
    info.setProperty("schema", "sales");
    var parsed =
        ConfluxQueryDriver.parse(
            "jdbc:qcli://gateway.example.com:32100/trino-prod?tls=false&connectTimeoutMs=2500",
            info);
    assertEquals("jdbc:arrow-flight-sql://gateway.example.com:32100/", parsed.delegateUrl());
    assertEquals("jdbc:qcli://gateway.example.com:32100/trino-prod", parsed.publicUrl());
    assertEquals("trino-prod", parsed.properties().getProperty("qcli-target"));
    assertEquals("false", parsed.properties().getProperty("useEncryption"));
    assertEquals("2500", parsed.properties().getProperty("connectTimeoutMs"));
    assertEquals("hive", parsed.properties().getProperty("catalog"));
    assertEquals("sales", parsed.schema());
    assertNull(parsed.properties().getProperty("schema"));
  }

  @Test
  void jwtIsASecretBearerAliasAndNeverAppearsInTranslatedUrl() throws Exception {
    Properties info = new Properties();
    info.setProperty("jwt", "signed-jwt");
    var parsed = ConfluxQueryDriver.parse("jdbc:qcli://gateway/demo", info);
    assertEquals("signed-jwt", parsed.properties().getProperty("token"));
    assertNull(parsed.properties().getProperty("jwt"));
    assertFalse(parsed.delegateUrl().contains("signed-jwt"));
    assertFalse(parsed.publicUrl().contains("signed-jwt"));
  }

  @Test
  void rejectsCredentialsAndUnknownValuesInUrl() {
    assertThrows(
        SQLNonTransientConnectionException.class,
        () -> ConfluxQueryDriver.parse("jdbc:qcli://gateway/demo?token=leak", new Properties()));
    assertThrows(
        SQLNonTransientConnectionException.class,
        () -> ConfluxQueryDriver.parse("jdbc:qcli://gateway/demo?unknown=true", new Properties()));
    assertThrows(
        SQLNonTransientConnectionException.class,
        () -> ConfluxQueryDriver.parse("jdbc:qcli://gateway/demo?tls=true&tls=false", new Properties()));
    assertThrows(
        SQLNonTransientConnectionException.class,
        () -> ConfluxQueryDriver.parse("jdbc:qcli://gateway/demo?tls=true&TLS=false", new Properties()));
  }

  @Test
  void rejectsMalformedOrAmbiguousTargets() {
    for (String url :
        new String[] {
          "jdbc:qcli://gateway",
          "jdbc:qcli://gateway/",
          "jdbc:qcli://gateway/one/two",
          "jdbc:qcli:///demo",
          "jdbc:qcli://user@gateway/demo",
          "jdbc:qcli://gateway/demo#fragment"
        }) {
      assertThrows(SQLNonTransientConnectionException.class, () -> ConfluxQueryDriver.parse(url, new Properties()), url);
    }
  }

  @Test
  void propertyInfoRedactsEveryCredential() throws Exception {
    Properties info = new Properties();
    info.setProperty("token", "must-not-escape");
    info.setProperty("trustStorePassword", "must-not-escape");
    DriverPropertyInfo[] properties = driver.getPropertyInfo(null, info);
    assertNull(find(properties, "token").value);
    assertNull(find(properties, "trustStorePassword").value);
    assertEquals("true", find(properties, "tls").value);
  }

  @Test
  void serviceProviderAndDriverManagerDiscoverBrandedDriver() throws Exception {
    assertTrue(
        ServiceLoader.load(Driver.class).stream()
            .anyMatch(provider -> provider.type().equals(ConfluxQueryDriver.class)));
    assertTrue(DriverManager.getDriver("jdbc:qcli://localhost/demo") instanceof ConfluxQueryDriver);
    assertNull(driver.connect("jdbc:other://localhost/demo", new Properties()));
  }

  private static DriverPropertyInfo find(DriverPropertyInfo[] values, String name) {
    return Arrays.stream(values).filter(value -> value.name.equals(name)).findFirst().orElseThrow();
  }
}
