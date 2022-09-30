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
package io.stargate.sgv2.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.sgv2.api.common.cql.builder.CollectionIndexingType;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;

public class CreateIndexFetcher extends IndexFetcher {

  @Override
  protected Query buildQuery(
      DataFetchingEnvironment environment, String keyspaceName, String tableName) {
    String columnName = environment.getArgument("columnName");

    String indexName = environment.getArgument("indexName");

    boolean ifNotExists = environment.getArgumentOrDefault("ifNotExists", Boolean.FALSE);
    String customIndexClass = environment.getArgumentOrDefault("indexType", null);

    String indexKind = environment.getArgumentOrDefault("indexKind", null);
    CollectionIndexingType indexingType = convertIndexingType(indexKind);

    return new QueryBuilder()
        .create()
        .index(indexName)
        .ifNotExists(ifNotExists)
        .on(keyspaceName, tableName)
        .column(columnName)
        .indexingType(indexingType)
        .custom(customIndexClass)
        .build();
  }

  private CollectionIndexingType convertIndexingType(String spec) {
    if (spec != null) {
      switch (spec) {
        case "KEYS":
          return CollectionIndexingType.KEYS;
        case "VALUES":
          return CollectionIndexingType.VALUES;
        case "ENTRIES":
          return CollectionIndexingType.ENTRIES;
        case "FULL":
          return CollectionIndexingType.FULL;
        default:
          throw new IllegalArgumentException(String.format("Invalid indexKind value: %s", spec));
      }
    }
    return null;
  }
}
