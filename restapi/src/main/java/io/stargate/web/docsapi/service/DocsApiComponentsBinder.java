package io.stargate.web.docsapi.service;

import io.dropwizard.setup.Environment;
import org.glassfish.jersey.internal.inject.AbstractBinder;

public class DocsApiComponentsBinder extends AbstractBinder {
  private final Environment environment;

  public DocsApiComponentsBinder(Environment environment) {
    this.environment = environment;
  }

  protected void configure() {
    DocsApiConfiguration conf = DocsApiConfiguration.DEFAULT;
    bind(conf).to(DocsApiConfiguration.class);
    bind(TimeSource.SYSTEM).to(TimeSource.class);

    bindAsContract(JsonConverter.class);
    bindAsContract(DocsSchemaChecker.class);
    bindAsContract(DocumentService.class);
    bindAsContract(CollectionService.class);
  }
}
