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

package io.stargate.sgv2.docsapi.api.common.properties.model;

import io.stargate.sgv2.docsapi.config.DocumentConfig;

/**
 * Joined {@link DocumentProperties} and {@link DataStoreProperties} that create a full docs api
 * properties.
 */
public class CombinedProperties implements DocumentProperties, DataStoreProperties {

  /** The document properties. */
  private final DocumentProperties documentProperties;

  /** The data store properties. */
  private final DataStoreProperties dataStoreProperties;

  /** Cached table columns. */
  private final DocumentTableColumns tableColumns;

  /**
   * Default constructor.
   *
   * @param documentProperties The document properties to delegate to.
   * @param dataStoreProperties The data store properties to delegate to.
   */
  public CombinedProperties(
      DocumentConfig documentProperties, DataStoreProperties dataStoreProperties) {
    this.documentProperties = documentProperties;
    this.dataStoreProperties = dataStoreProperties;
    this.tableColumns = new DocumentTableColumns(documentProperties);
  }

  // below are delegates to config and properties

  /** {@inheritDoc} */
  @Override
  public int maxDepth() {
    return documentProperties.maxDepth();
  }

  /** {@inheritDoc} */
  @Override
  public int maxArrayLength() {
    return documentProperties.maxArrayLength();
  }

  /** {@inheritDoc} */
  @Override
  public int maxPageSize() {
    return documentProperties.maxPageSize();
  }

  /** {@inheritDoc} */
  @Override
  public int searchPageSize() {
    return documentProperties.searchPageSize();
  }

  /** {@inheritDoc} */
  @Override
  public DocumentTableProperties table() {
    return documentProperties.table();
  }

  /** {@inheritDoc} */
  @Override
  public DocumentTableColumns tableColumns() {
    return tableColumns;
  }

  /** {@inheritDoc} */
  @Override
  public boolean secondaryIndexesEnabled() {
    return dataStoreProperties.secondaryIndexesEnabled();
  }

  /** {@inheritDoc} */
  @Override
  public boolean saiEnabled() {
    return dataStoreProperties.saiEnabled();
  }

  /** {@inheritDoc} */
  @Override
  public boolean loggedBatchesEnabled() {
    return dataStoreProperties.loggedBatchesEnabled();
  }
}
