package io.stargate.web.docsapi.exception;

public class DocumentAPIRequestException extends RuntimeException {
  public DocumentAPIRequestException(String msg) {
    super(msg);
  }

  public DocumentAPIRequestException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
