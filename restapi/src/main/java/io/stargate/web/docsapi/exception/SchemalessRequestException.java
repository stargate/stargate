package io.stargate.web.docsapi.exception;

public class SchemalessRequestException extends RuntimeException {
  public SchemalessRequestException(String msg) {
    super(msg);
  }

  public SchemalessRequestException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
