package io.stargate.db.exceptions;

/**
 * An exception to signal that an instance of {@link io.stargate.db.Persistence} is unable to handle
 * a particular client's requests.
 */
public class UnhandledClientException extends RuntimeException {

  public UnhandledClientException() {}

  public UnhandledClientException(String message) {
    super(message);
  }
}
