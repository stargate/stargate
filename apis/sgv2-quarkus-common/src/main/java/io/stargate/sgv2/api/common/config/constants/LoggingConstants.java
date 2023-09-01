package io.stargate.sgv2.api.common.config.constants;

public interface LoggingConstants {
  /** The default value for the {@link LoggingConfig#enabled()} configuration. */
  public static final String REQUEST_INFO_LOGGING_ENABLED = "false";
  /** The default value for the {@link LoggingConfig#enabledTenants()} configuration. */
  public static final String ALL_TENANTS = "all";
  /** The default value for the {@link LoggingConfig#enabledPaths()} configuration. */
  public static final String ALL_PATHS = "all";
  /** The default value for the {@link LoggingConfig#enabledPathPrefixes()} configuration. */
  public static final String ALL_PATH_PREFIXES = "*";
  /** The default value for the {@link LoggingConfig#enabledErrorCodes()} configuration. */
  public static final String ALL_ERROR_CODES = "all";
  /** The default value for the {@link LoggingConfig#enabledMethods()} configuration. */
  public static final String ALL_METHODS = "all";
  /** The default value for the {@link LoggingConfig#requestBodyLoggingEnabled()} configuration. */
  public static final String REQUEST_BODY_LOGGING_ENABLED = "false";
}
