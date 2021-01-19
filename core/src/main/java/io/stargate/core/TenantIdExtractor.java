package io.stargate.core;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantIdExtractor {
  private static final Logger log = LoggerFactory.getLogger(TenantIdExtractor.class);

  /**
   * The Host header is in the following format: Host: uuid-region.apps.astra.datastax.com:123 We
   * need to extract the uuid from it, thus taking the first 36 characters.
   */
  private static String extractTenantIdFromHostHeader(String hostHeader) {
    if (hostHeader == null) {
      return null;
    }
    if (hostHeader.length() < 36) {
      return null;
    }
    String tenantIdCandidate = hostHeader.substring(0, 36);
    // create UUID from tenant id to assure that it is in a proper format
    try {
      UUID uuid = UUID.fromString(tenantIdCandidate);
      return uuid.toString();
    } catch (IllegalArgumentException exception) {
      log.debug("problem when parsing UUID from the host header: " + hostHeader, exception);
      return null;
    }
  }
}
