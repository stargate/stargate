package io.stargate.sgv2.docsapi.api.common.properties.document.impl;

import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.config.DocumentConfig;

/**
 * Immutable implementation of the {@link DocumentProperties}.
 *
 * @see DocumentProperties
 */
public record DocumentPropertiesImpl(
    int maxDepth,
    int maxArrayLength,
    int maxPageSize,
    int maxSearchPageSize,
    DocumentTableProperties tableProperties,
    DocumentTableColumns tableColumns)
    implements DocumentProperties {

  public DocumentPropertiesImpl(DocumentConfig documentConfig, boolean numericBooleans) {
    this(
        documentConfig.maxDepth(),
        documentConfig.maxArrayLength(),
        documentConfig.maxPageSize(),
        documentConfig.maxSearchPageSize(),
        new DocumentTablePropertiesImpl(documentConfig),
        new DocumentTableColumnsImpl(documentConfig, numericBooleans));
  }
}
