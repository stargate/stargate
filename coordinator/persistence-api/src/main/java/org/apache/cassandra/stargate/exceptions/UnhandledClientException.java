package org.apache.cassandra.stargate.exceptions;

/**
 * An exception to signal that an instance of {@link io.stargate.db.Persistence} is unable to handle
 * a particular client's requests.
 */
public class UnhandledClientException extends PersistenceException {

  public UnhandledClientException(String message) {
    super(ExceptionCode.SERVER_ERROR, message);
  }
}
