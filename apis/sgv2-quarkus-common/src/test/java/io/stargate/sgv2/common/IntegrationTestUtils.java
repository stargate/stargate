package io.stargate.sgv2.common;

import java.net.InetSocketAddress;
import java.util.Objects;

/** Utilities for integration test. */
public final class IntegrationTestUtils {

  public static final String AUTH_TOKEN_PROP = "stargate.int-test.auth-token";
  public static final String CASSANDRA_HOST_PROP = "stargate.int-test.cassandra.host";
  public static final String CASSANDRA_CQL_PORT_PROP = "stargate.int-test.cassandra.cql-port";

  public static final String CLUSTER_VERSION_PROP = "stargate.int-test.cluster-version";

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

  /** @return Returns the cluster version (3.11, 4.0, 6.8 (== DSE)) specified for the coordinator */
  public static String getClusterVersion() {
    return System.getProperty(CLUSTER_VERSION_PROP, "");
  }

  /**
   * @return True if the backend cluster is DSE-based (including C2 and CNDB), false if OSS
   *     Cassandra (3.11, 4.0)
   */
  public static boolean isDSE() {
    return "6.8".equals(getClusterVersion());
  }

  /** @return True if the backend cluster is Cassandra 4.0; false otherwise (DSE, C-3.11) */
  public static boolean isCassandra40() {
    return "4.0".equals(getClusterVersion());
  }
}
