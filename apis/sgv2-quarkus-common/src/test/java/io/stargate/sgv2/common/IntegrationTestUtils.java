package io.stargate.sgv2.common;

/** Utilities for integration test. */
public final class IntegrationTestUtils {

  public static final String AUTH_TOKEN_PROP = "stargate.int-test.auth-token";

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

  /**
   * @return Returns the cluster version (3.11, 4.0, 6.8 (== DSE)) specified for the coordinator
   */
  public static String getClusterVersion() {
    return System.getProperty(CLUSTER_VERSION_PROP, "");
  }

  /**
   * Convenience method for detecting cases where backend storage cluster is DSE (or DSE-based).
   *
   * @return True if the backend cluster is DSE-based (including C2 and CNDB), false if OSS Cassandra (3.11, 4.0)
   */
  public static boolean isDSE() {
    return "6.8".equals(getClusterVersion());
  }
}
