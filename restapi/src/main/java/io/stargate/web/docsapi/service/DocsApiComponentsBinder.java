package io.stargate.web.docsapi.service;

import io.dropwizard.setup.Environment;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.condition.ConditionParser;
import org.glassfish.jersey.internal.inject.AbstractBinder;

public class DocsApiComponentsBinder extends AbstractBinder {
  private final Environment environment;

  public DocsApiComponentsBinder(Environment environment) {
    this.environment = environment;
  }

  protected void configure() {
    bind(TimeSource.SYSTEM).to(TimeSource.class);

    bindAsContract(JsonConverter.class);
    bindAsContract(DocsShredder.class);
    bindAsContract(DocsSchemaChecker.class);
    bindAsContract(DocumentService.class);
    bindAsContract(CollectionService.class);
    bindAsContract(JsonSchemaHandler.class);
    bindAsContract(ExpressionParser.class);
    bindAsContract(ConditionParser.class);
    bindAsContract(DocumentSearchService.class);
    bindAsContract(ReactiveDocumentService.class);
  }
}
