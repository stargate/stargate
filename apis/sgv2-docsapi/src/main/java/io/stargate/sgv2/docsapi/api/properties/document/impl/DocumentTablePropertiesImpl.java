/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.properties.document.impl;

import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
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
