package io.stargate.web.docsapi.exception;

import org.jsfr.json.ErrorHandlingStrategy;
import org.jsfr.json.ParsingContext;

/** {@link ErrorHandlingStrategy} that re-throws the RuntimeExceptions */
public class RuntimeExceptionPassHandlingStrategy implements ErrorHandlingStrategy {

  @Override
  public void handleParsingException(Exception e) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }

  @Override
  public void handleExceptionFromListener(Exception e, ParsingContext context) {
    if (e instanceof RuntimeException) {
      throw (RuntimeException) e;
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }
}
