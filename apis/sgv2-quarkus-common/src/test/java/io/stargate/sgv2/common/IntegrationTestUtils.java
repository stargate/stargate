package io.stargate.sgv2.common;

import java.net.InetSocketAddress;
import java.util.Objects;
import org.eclipse.microprofile.config.ConfigProvider;

/** Utilities for integration test. */
public final class IntegrationTestUtils {

  public static final String AUTH_TOKEN_PROP = "stargate.int-test.auth-token";
  public static final String CASSANDRA_HOST_PROP = "stargate.int-test.cassandra.host";
  public static final String CASSANDRA_CQL_PORT_PROP = "stargate.int-test.cassandra.cql-port";
  public static final String CASSANDRA_AUTH_ENABLED_PROP =
      "stargate.int-test.cassandra.auth-enabled";
  public static final String CASSANDRA_USERNAME_PROP = "stargate.int-test.cassandra.username";
  public static final String CASSANDRA_PASSWORD_PROP = "stargate.int-test.cassandra.password";
  public static final String PERSISTENCE_MODULE_PROP = "stargate.int-test.cluster.persistence";

  private IntegrationTestUtils() {}

  /**
   * @return Returns the auth token from the system property {@value AUTH_TOKEN_PROP}, returning
   *     empty string if property is not defined.
   */
  public static String getAuthToken() {
    return getAuthToken("");
  }

  /**
   * @return Returns the auth token from the system property {@value AUTH_TOKEN_PROP}, returning
   *     passed default if property is not defined.
   */
  public static String getAuthToken(String defaultIfMissing) {
    return System.getProperty(AUTH_TOKEN_PROP, defaultIfMissing);
  }

  /** @return the CQL address of the Cassandra backend. */
  public static InetSocketAddress getCassandraCqlAddress() {
    String host =
        Objects.requireNonNull(
            System.getProperty(CASSANDRA_HOST_PROP),
            "Expected system property %s to be set".formatted(CASSANDRA_HOST_PROP));
    Integer port = Integer.getInteger(CASSANDRA_CQL_PORT_PROP);
    Objects.requireNonNull(
        port,
        "Expected system property %s to be set to an integer (got %s)"
            .formatted(CASSANDRA_CQL_PORT_PROP, System.getProperty(CASSANDRA_CQL_PORT_PROP)));
    return new InetSocketAddress(host, port);
  }

  /** @return If Cassandra auth is enabled */
  public static boolean isCassandraAuthEnabled() {
    return Boolean.parseBoolean(System.getProperty(CASSANDRA_AUTH_ENABLED_PROP, "false"));
  }

  /** @return Cassandra username, only meaningful if Cassandra auth is enabled */
  public static String getCassandraUsername() {
    return System.getProperty(CASSANDRA_USERNAME_PROP, "cassandra");
  }

  /** @return Cassandra password, only meaningful if Cassandra auth is enabled */
  public static String getCassandraPassword() {
    return System.getProperty(CASSANDRA_PASSWORD_PROP, "cassandra");
  }

  /**
   * @return Returns the persistence module used by the coordinator (4.0, 6.8 (== DSE)) specified
   *     for the coordinator
   */
  public static String getPersistenceModule() {
    return System.getProperty(PERSISTENCE_MODULE_PROP, "");
  }

  /**
   * @return True if the backend cluster is DSE-based (including C2 and CNDB), false if OSS
   *     Cassandra (4.0)
   */
  public static boolean isDSE() {
    return "persistence-dse-6.8".equals(getPersistenceModule());
  }

  /**
   * @return True if the backend cluster supports SASI (currently only enabled by default in DSE,
   *     not OSS Cassandra backends)
   */
  public static boolean supportsSASI() {
    return isDSE();
  }

  /**
   * @return True if the backend cluster supports Materialized Views (currently only enabled by
   *     default in DSE, not OSS Cassandra backends)
   */
  public static boolean supportsMaterializedViews() {
    return isDSE();
  }

  /** @return Returns the port where the application to test runs. */
  public static int getTestPort() {
    try {
      return ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
    } catch (Exception e) {
      return Integer.parseInt(System.getProperty("quarkus.http.test-port"));
    }
  }
}
