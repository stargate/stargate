/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.solr.common.SolrException;

final class SolrRequestExecutionException extends RequestExecutionException {
  public SolrRequestExecutionException(String msg, Throwable cause) {
    super(translateException(cause), msg, cause);
  }

  private static ExceptionCode translateException(Throwable ex) {
    if (ex instanceof SolrException) {
      SolrException.ErrorCode solrErrorCode =
          SolrException.ErrorCode.getErrorCode(((SolrException) ex).code());
      switch (solrErrorCode) {
        case BAD_REQUEST:
        case CONFLICT:
        case NOT_FOUND:
        case UNSUPPORTED_MEDIA_TYPE:
          return ExceptionCode.INVALID;
        case UNAUTHORIZED:
          return ExceptionCode.UNAUTHORIZED;
        case FORBIDDEN:
          return ExceptionCode.BAD_CREDENTIALS;
        case SERVICE_UNAVAILABLE:
          return ExceptionCode.UNAVAILABLE;
      }
    }
    return ExceptionCode.SERVER_ERROR;
  }
}
