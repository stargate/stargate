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

import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableColumns;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentTableProperties;
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
        DocumentTableColumnsImpl.of(documentConfig, numericBooleans));
  }
}
