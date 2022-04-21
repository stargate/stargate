package io.stargate.sgv2.docsapi.api.common.properties.document.configuration;

import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.impl.DocumentPropertiesImpl;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

/** Producer of the {@link DocumentProperties}. */
public class DocumentPropertiesConfiguration {

  @Produces
  @Singleton
  public DocumentProperties documentProperties(DocumentConfig documentConfig) {
    return new DocumentPropertiesImpl(documentConfig);
  }
}
