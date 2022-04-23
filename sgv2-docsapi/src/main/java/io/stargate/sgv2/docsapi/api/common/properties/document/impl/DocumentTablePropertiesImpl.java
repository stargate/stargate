package io.stargate.sgv2.docsapi.api.common.properties.document.impl;

import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentTableProperties;
import io.stargate.sgv2.docsapi.config.DocumentConfig;

/**
 * Immutable implementation of the {@link DocumentTableProperties}.
 *
 * @see DocumentTableProperties
 */
public record DocumentTablePropertiesImpl(
    String keyColumnName,
    String leafColumnName,
    String stringValueColumnName,
    String doubleValueColumnName,
    String booleanValueColumnName,
    String pathColumnPrefix)
    implements DocumentTableProperties {

  /**
   * Helper constructor to construct instance based on the configuration.
   *
   * @param documentConfig Properties based configuration.
   */
  public DocumentTablePropertiesImpl(DocumentConfig documentConfig) {
    this(documentConfig.table());
  }

  /**
   * Private constructor to construct instance based on the table configuration.
   *
   * @param tableConfig Properties based table configuration.
   */
  private DocumentTablePropertiesImpl(DocumentConfig.DocumentTableConfig tableConfig) {
    this(
        tableConfig.keyColumnName(),
        tableConfig.leafColumnName(),
        tableConfig.stringValueColumnName(),
        tableConfig.doubleValueColumnName(),
        tableConfig.booleanValueColumnName(),
        tableConfig.pathColumnPrefix());
  }
}
