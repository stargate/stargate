package io.stargate.sgv2.docsapi.api.common.properties.document.configuration;

import io.quarkus.runtime.Startup;
import io.stargate.sgv2.docsapi.api.common.properties.datastore.DataStoreProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.impl.DocumentPropertiesImpl;
import io.stargate.sgv2.docsapi.config.DocumentConfig;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/** Producer of the {@link DocumentProperties}. */
public class DocumentPropertiesConfiguration {

  @Produces
  @ApplicationScoped
  @Startup
  public DocumentProperties documentProperties(
      DocumentConfig documentConfig, DataStoreProperties properties) {
    return new DocumentPropertiesImpl(documentConfig, properties.treatBooleansAsNumeric());
  }
}
