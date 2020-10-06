package io.stargate.web.docsapi.exception;

import org.jsfr.json.ErrorHandlingStrategy;
import org.jsfr.json.ParsingContext;

public class SchemalessErrorHandlingStrategy implements ErrorHandlingStrategy {
  @Override
  public void handleParsingException(Exception e) {
    if (e instanceof SchemalessRequestException) {
      throw (SchemalessRequestException) e;
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }

  @Override
  public void handleExceptionFromListener(Exception e, ParsingContext context) {
    if (e instanceof SchemalessRequestException) {
      throw (SchemalessRequestException) e;
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }
}
