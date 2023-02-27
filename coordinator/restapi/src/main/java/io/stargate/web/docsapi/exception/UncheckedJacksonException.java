package io.stargate.web.docsapi.exception;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Simple unchecked exception wrapper needed to expose Jackson-originated exceptions via JsonSurfer
 * handlers, while making it easier to recognize the underlying type (usually to avoid things like
 * printing stack traces for "well-known" failure types).
 */
public class UncheckedJacksonException extends RuntimeException {
  public UncheckedJacksonException(JsonProcessingException cause) {
    super(cause);
  }

  public UncheckedJacksonException(String messsage, JsonProcessingException cause) {
    super(messsage, cause);
  }
}
