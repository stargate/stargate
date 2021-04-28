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
package io.stargate.graphql.schema.graphqlfirst.processor;

import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.ImmutableCollectionIndexingType;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum IndexTarget {
  KEYS(ImmutableCollectionIndexingType.builder().indexKeys(true).build()),
  VALUES(ImmutableCollectionIndexingType.builder().indexValues(true).build()),
  ENTRIES(ImmutableCollectionIndexingType.builder().indexEntries(true).build()),
  FULL(ImmutableCollectionIndexingType.builder().indexFull(true).build()),
  ;

  private static final Map<CollectionIndexingType, IndexTarget> BY_TYPE =
      Arrays.stream(values())
          .collect(Collectors.toMap(IndexTarget::toIndexingType, Function.identity()));

  private final CollectionIndexingType indexingType;

  IndexTarget(CollectionIndexingType indexingType) {
    this.indexingType = indexingType;
  }

  public CollectionIndexingType toIndexingType() {
    return indexingType;
  }

  public static IndexTarget fromIndexingType(CollectionIndexingType indexingType) {
    IndexTarget value = BY_TYPE.get(indexingType);
    if (value == null) {
      throw new IllegalArgumentException("Invalid value " + indexingType);
    }
    return value;
  }
}
