package io.stargate.web.docsapi.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.jsfr.json.ErrorHandlingStrategy;
import org.jsfr.json.ParsingContext;

/** {@link ErrorHandlingStrategy} that re-throws the RuntimeExceptions */
public class RuntimeExceptionPassHandlingStrategy implements ErrorHandlingStrategy {

  @Override
  public void handleParsingException(Exception e) {
    throw translate(e);
  }

  @Override
  public void handleExceptionFromListener(Exception e, ParsingContext context) {
    throw translate(e);
  }

  private RuntimeException translate(Exception e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    if (e instanceof JsonProcessingException) { // from Jackson
      throw new UncheckedJacksonException((JsonProcessingException) e);
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }
}
