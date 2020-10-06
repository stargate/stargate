package io.stargate.web.docsapi.exception;

import org.jsfr.json.ErrorHandlingStrategy;
import org.jsfr.json.ParsingContext;

public class DocumentAPIErrorHandlingStrategy implements ErrorHandlingStrategy {
  @Override
  public void handleParsingException(Exception e) {
    if (e instanceof DocumentAPIRequestException) {
      throw (DocumentAPIRequestException) e;
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }

  @Override
  public void handleExceptionFromListener(Exception e, ParsingContext context) {
    if (e instanceof DocumentAPIRequestException) {
      throw (DocumentAPIRequestException) e;
    }
    throw new RuntimeException(e.getLocalizedMessage(), e);
  }
}
