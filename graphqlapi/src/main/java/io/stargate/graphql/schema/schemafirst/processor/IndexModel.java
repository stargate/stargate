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
package io.stargate.graphql.schema.schemafirst.processor;

import io.stargate.db.schema.CollectionIndexingType;
import java.util.Map;
import java.util.Optional;

public class IndexModel {

  private final String name;
  private final Optional<String> indexClass;
  private final CollectionIndexingType indexingType;
  private final Map<String, String> options;

  IndexModel(
      String name,
      Optional<String> indexClass,
      CollectionIndexingType indexingType,
      Map<String, String> options) {
    this.name = name;
    this.indexClass = indexClass;
    this.indexingType = indexingType;
    this.options = options;
  }

  public String getName() {
    return name;
  }

  public Optional<String> getIndexClass() {
    return indexClass;
  }

  public CollectionIndexingType getIndexingType() {
    return indexingType;
  }

  public Map<String, String> getOptions() {
    return options;
  }
}
