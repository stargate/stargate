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
    JsonConverter jsonConverter = new JsonConverter(environment.getObjectMapper(), conf);
    bind(conf).to(DocsApiConfiguration.class);
    bind(jsonConverter).to(JsonConverter.class);
    bind(new DocumentService(environment.getObjectMapper(), jsonConverter, conf))
        .to(DocumentService.class);
  }
}
