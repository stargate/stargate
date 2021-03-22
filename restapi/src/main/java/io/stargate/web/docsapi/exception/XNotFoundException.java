package io.stargate.web.docsapi.exception;

/** Thrown when API can not find a requested resource. */
public class XNotFoundException extends RuntimeException {
  public XNotFoundException(String msg) {
    super(msg);
  }

  public XNotFoundException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
