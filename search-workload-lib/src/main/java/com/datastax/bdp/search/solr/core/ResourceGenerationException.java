/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core;

public class ResourceGenerationException extends RuntimeException {
  public ResourceGenerationException(String message) {
    super(message);
  }

  public ResourceGenerationException(String message, Throwable cause) {
    super(message, cause);
  }
}
