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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import io.stargate.bridge.proto.Schema.IndexingType;
import java.util.Map;
import java.util.Optional;

public class IndexModel {

  public static final String SAI_INDEX_CLASS_NAME =
      "org.apache.cassandra.index.sai.StorageAttachedIndex";

  private final String name;
  private final Optional<String> indexClass;
  private final IndexingType indexingType;
  private final Map<String, String> options;

  IndexModel(
      String name,
      Optional<String> indexClass,
      IndexingType indexingType,
      Map<String, String> options) {
    this.name = name;
    this.indexClass = indexClass;
    this.indexingType = indexingType;
    this.options = options;
  }

  public String getName() {
    return name;
  }

  /**
   * The name of the index class if this is a custom index, or empty if this is a regular secondary
   * index.
   */
  public Optional<String> getIndexClass() {
    return indexClass;
  }

  /**
   * Whether the index is a known implementation (non-custom or SAI).
   *
   * <p>When this is true, we can perform pre-checks on query conditions, e.g. which value type is
   * expected for certain operators.
   */
  public boolean isBuiltIn() {
    return getIndexClass().map(c -> c.equals(SAI_INDEX_CLASS_NAME)).orElse(true);
  }

  public IndexingType getIndexingType() {
    return indexingType;
  }

  public Map<String, String> getOptions() {
    return options;
  }
}
