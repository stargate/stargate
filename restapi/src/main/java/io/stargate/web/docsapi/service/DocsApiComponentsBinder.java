package io.stargate.web.docsapi.service;

import io.stargate.core.util.TimeSource;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.condition.ConditionParser;
import io.stargate.web.docsapi.service.write.DocumentWriteService;
import javax.inject.Singleton;
import org.glassfish.jersey.internal.inject.AbstractBinder;

public class DocsApiComponentsBinder extends AbstractBinder {

  protected void configure() {
    bind(TimeSource.SYSTEM).to(TimeSource.class).in(Singleton.class);

    // services
    bindAsContract(JsonConverter.class).in(Singleton.class);
    bindAsContract(JsonDocumentShredder.class).in(Singleton.class);
    bindAsContract(DocsShredder.class).in(Singleton.class);
    bindAsContract(DocsSchemaChecker.class).in(Singleton.class);
    bindAsContract(CollectionService.class).in(Singleton.class);
    bindAsContract(JsonSchemaHandler.class).in(Singleton.class);
    bindAsContract(ExpressionParser.class).in(Singleton.class);
    bindAsContract(ConditionParser.class).in(Singleton.class);
    bindAsContract(DocumentSearchService.class).in(Singleton.class);
    bindAsContract(DocumentWriteService.class).in(Singleton.class);
    bindAsContract(ReactiveDocumentService.class).in(Singleton.class);
  }
}
