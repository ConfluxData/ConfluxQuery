package in.confluxdata.query.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

/** Type 4 JDBC driver for ConfluxQuery Gateway. */
public final class ConfluxQueryDriver implements Driver {
  /** Stable ConfluxQuery JDBC URL prefix. */
  public static final String URL_PREFIX = "jdbc:qcli://";
  private static final String ARROW_PREFIX = "jdbc:arrow-flight-sql://";
  private static final String DRIVER_NAME = "ConfluxQuery JDBC Driver";
  private static final Set<String> URL_SECRETS =
      Set.of(
          "token",
          "jwt",
          "password",
          "truststorepassword",
          "oauth.clientsecret",
          "oauth.exchange.subjecttoken",
          "oauth.exchange.actortoken");
  private static final Set<String> URL_PROPERTIES =
      Set.of(
          "tls",
          "certificateverification",
          "truststore",
          "usesystemtruststore",
          "tlsrootcerts",
          "clientcertificate",
          "clientkey",
          "connecttimeoutms",
          "retaincookies",
          "retainauth",
          "catalog",
          "schema");
  private static final List<PropertyDefinition> PROPERTIES =
      List.of(
          new PropertyDefinition("token", "Opaque API key or bearer token", true, null),
          new PropertyDefinition("jwt", "OIDC JWT bearer token (alias for token)", true, null),
          new PropertyDefinition("catalog", "Initial engine catalog/database", false, null),
          new PropertyDefinition("schema", "Initial engine schema", false, null),
          new PropertyDefinition("tls", "Encrypt Flight SQL transport", false, "true"),
          new PropertyDefinition(
              "certificateVerification", "Verify the Gateway certificate", false, "true"),
          new PropertyDefinition("trustStore", "Java trust-store path", false, null),
          new PropertyDefinition("trustStorePassword", "Java trust-store password", true, null),
          new PropertyDefinition("useSystemTrustStore", "Use system certificate trust", false, null),
          new PropertyDefinition("tlsRootCerts", "PEM root certificate path", false, null),
          new PropertyDefinition("clientCertificate", "mTLS client certificate path", false, null),
          new PropertyDefinition("clientKey", "mTLS client private-key path", true, null),
          new PropertyDefinition("connectTimeoutMs", "Connection timeout in milliseconds", false, "10000"),
          new PropertyDefinition("retainCookies", "Retain Flight session cookies", false, "true"),
          new PropertyDefinition("retainAuth", "Retain authorization across calls", false, "true"));

  static {
    try {
      DriverManager.registerDriver(new ConfluxQueryDriver());
    } catch (SQLException error) {
      throw new ExceptionInInitializerError(error);
    }
  }

  /** Creates a JDBC driver. Applications normally use service-provider discovery. */
  public ConfluxQueryDriver() {}

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    ParsedUrl parsed = parse(url, info);
    ArrowFlightJdbcDriver delegate = new ArrowFlightJdbcDriver();
    Connection connection = delegate.connect(parsed.delegateUrl(), parsed.properties());
    if (connection == null) {
      throw new SQLNonTransientConnectionException("Flight SQL delegate rejected the translated URL");
    }
    try {
      String catalog = parsed.properties().getProperty("catalog");
      if (catalog != null && !catalog.isBlank()) {
        connection.setCatalog(catalog);
      }
      String schema = parsed.schema();
      if (schema != null && !schema.isBlank()) {
        connection.setSchema(schema);
      }
      return wrapConnection(connection, parsed.publicUrl());
    } catch (SQLException error) {
      try {
        connection.close();
      } catch (SQLException closeError) {
        error.addSuppressed(closeError);
      }
      throw error;
    }
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Properties supplied = info == null ? new Properties() : info;
    List<DriverPropertyInfo> result = new ArrayList<>();
    for (PropertyDefinition definition : PROPERTIES) {
      String value = definition.secret() ? null : supplied.getProperty(definition.name(), definition.defaultValue());
      DriverPropertyInfo property = new DriverPropertyInfo(definition.name(), value);
      property.description = definition.description();
      property.required = definition.name().equals("token") && supplied.getProperty("jwt") == null;
      if (definition.name().equals("tls") || definition.name().equals("certificateVerification")
          || definition.name().equals("retainCookies") || definition.name().equals("retainAuth")) {
        property.choices = new String[] {"true", "false"};
      }
      result.add(property);
    }
    return result.toArray(DriverPropertyInfo[]::new);
  }

  @Override
  public int getMajorVersion() {
    return versionPart(0);
  }

  @Override
  public int getMinorVersion() {
    return versionPart(1);
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return Logger.getLogger(ConfluxQueryDriver.class.getPackageName());
  }

  static ParsedUrl parse(String url, Properties info) throws SQLException {
    if (url == null || !url.startsWith(URL_PREFIX)) {
      throw new SQLNonTransientConnectionException("URL must start with jdbc:qcli://");
    }
    final URI uri;
    try {
      uri = new URI(url.substring("jdbc:".length()));
    } catch (URISyntaxException error) {
      throw new SQLNonTransientConnectionException("Invalid jdbc:qcli URL", error);
    }
    if (!"qcli".equals(uri.getScheme()) || uri.getHost() == null || uri.getHost().isBlank()) {
      throw new SQLNonTransientConnectionException("jdbc:qcli URL requires a Gateway host");
    }
    if (uri.getUserInfo() != null || uri.getFragment() != null) {
      throw new SQLNonTransientConnectionException("jdbc:qcli URL cannot contain user info or a fragment");
    }
    String path = uri.getPath();
    if (path == null || path.length() < 2 || path.substring(1).contains("/")) {
      throw new SQLNonTransientConnectionException("jdbc:qcli URL requires exactly one target path segment");
    }
    String target = path.substring(1);
    if (target.isBlank()) {
      throw new SQLNonTransientConnectionException("jdbc:qcli target cannot be empty");
    }
    int port = uri.getPort() == -1 ? 32010 : uri.getPort();
    if (port < 1 || port > 65535) {
      throw new SQLNonTransientConnectionException("jdbc:qcli Gateway port is out of range");
    }

    Map<String, String> query = parseQuery(uri.getRawQuery());
    Properties properties = new Properties();
    for (Map.Entry<String, String> entry : query.entrySet()) {
      String normalized = entry.getKey().toLowerCase(Locale.ROOT);
      if (URL_SECRETS.contains(normalized)) {
        throw new SQLNonTransientConnectionException(
            "Credentials are not allowed in a jdbc:qcli URL; use connection properties");
      }
      if (!URL_PROPERTIES.contains(normalized)) {
        throw new SQLNonTransientConnectionException("Unsupported jdbc:qcli URL property: " + entry.getKey());
      }
      properties.setProperty(canonicalProperty(entry.getKey()), entry.getValue());
    }
    if (info != null) {
      properties.putAll(info);
    }
    String jwt = properties.getProperty("jwt");
    String token = properties.getProperty("token");
    if (jwt != null && token != null && !jwt.equals(token)) {
      throw new SQLNonTransientConnectionException("Specify only one bearer credential: token or jwt");
    }
    if (jwt != null) {
      properties.setProperty("token", jwt);
    }
    properties.remove("jwt");
    String schema = properties.getProperty("schema");
    properties.remove("schema");
    String tls = properties.getProperty("tls", "true");
    properties.remove("tls");
    properties.setProperty("useEncryption", tls);
    properties.putIfAbsent("connectTimeoutMs", "10000");
    properties.putIfAbsent("retainCookies", "true");
    properties.putIfAbsent("retainAuth", "true");
    properties.setProperty("qcli-target", target);

    String host = uri.getHost().contains(":") ? "[" + uri.getHost() + "]" : uri.getHost();
    String delegateUrl = ARROW_PREFIX + host + ":" + port + "/";
    String publicUrl = URL_PREFIX + host + ":" + port + "/" + target;
    return new ParsedUrl(delegateUrl, publicUrl, target, schema, properties);
  }

  private static Map<String, String> parseQuery(String rawQuery) throws SQLException {
    if (rawQuery == null || rawQuery.isBlank()) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new LinkedHashMap<>();
    Set<String> normalizedNames = new java.util.HashSet<>();
    for (String pair : rawQuery.split("&")) {
      int separator = pair.indexOf('=');
      if (separator < 1) {
        throw new SQLNonTransientConnectionException("jdbc:qcli URL query properties require name=value");
      }
      String name = URLDecoder.decode(pair.substring(0, separator), StandardCharsets.UTF_8);
      String value = URLDecoder.decode(pair.substring(separator + 1), StandardCharsets.UTF_8);
      if (!normalizedNames.add(name.toLowerCase(Locale.ROOT))) {
        throw new SQLNonTransientConnectionException("Duplicate jdbc:qcli URL property: " + name);
      }
      result.put(name, value);
    }
    return result;
  }

  private static String canonicalProperty(String name) {
    return switch (name.toLowerCase(Locale.ROOT)) {
      case "certificateverification" -> "certificateVerification";
      case "truststore" -> "trustStore";
      case "usesystemtruststore" -> "useSystemTrustStore";
      case "tlsrootcerts" -> "tlsRootCerts";
      case "clientcertificate" -> "clientCertificate";
      case "clientkey" -> "clientKey";
      case "connecttimeoutms" -> "connectTimeoutMs";
      case "retaincookies" -> "retainCookies";
      case "retainauth" -> "retainAuth";
      default -> name.toLowerCase(Locale.ROOT);
    };
  }

  private static Connection wrapConnection(Connection delegate, String publicUrl) {
    return (Connection)
        Proxy.newProxyInstance(
            ConfluxQueryDriver.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, arguments) -> {
              if (method.getName().equals("getMetaData") && method.getParameterCount() == 0) {
                return wrapMetadata(delegate.getMetaData(), publicUrl);
              }
              if (method.getName().equals("isWrapperFor") && method.getParameterCount() == 1) {
                Class<?> type = (Class<?>) arguments[0];
                return type.isInstance(proxy) || type.isInstance(delegate) || delegate.isWrapperFor(type);
              }
              if (method.getName().equals("unwrap") && method.getParameterCount() == 1) {
                Class<?> type = (Class<?>) arguments[0];
                if (type.isInstance(proxy)) return proxy;
                if (type.isInstance(delegate)) return type.cast(delegate);
                return delegate.unwrap(type);
              }
              return invoke(delegate, method, arguments);
            });
  }

  private static DatabaseMetaData wrapMetadata(DatabaseMetaData delegate, String publicUrl) {
    return (DatabaseMetaData)
        Proxy.newProxyInstance(
            ConfluxQueryDriver.class.getClassLoader(),
            new Class<?>[] {DatabaseMetaData.class},
            (proxy, method, arguments) ->
                switch (method.getName()) {
                  case "getDriverName" -> DRIVER_NAME;
                  case "getDriverVersion" -> version();
                  case "getDriverMajorVersion" -> versionPart(0);
                  case "getDriverMinorVersion" -> versionPart(1);
                  case "getURL" -> publicUrl;
                  case "getConnection" -> wrapConnection(delegate.getConnection(), publicUrl);
                  default -> invoke(delegate, method, arguments);
                });
  }

  private static Object invoke(Object delegate, java.lang.reflect.Method method, Object[] arguments)
      throws Throwable {
    try {
      return method.invoke(delegate, arguments);
    } catch (InvocationTargetException error) {
      throw error.getCause();
    }
  }

  private static String version() {
    String value = ConfluxQueryDriver.class.getPackage().getImplementationVersion();
    return value == null ? "0.1.0" : value;
  }

  private static int versionPart(int index) {
    String[] parts = version().split("[.-]");
    if (index >= parts.length) return 0;
    try {
      return Integer.parseInt(parts[index]);
    } catch (NumberFormatException ignored) {
      return 0;
    }
  }

  record ParsedUrl(String delegateUrl, String publicUrl, String target, String schema, Properties properties) {}

  private record PropertyDefinition(
      String name, String description, boolean secret, String defaultValue) {}
}
