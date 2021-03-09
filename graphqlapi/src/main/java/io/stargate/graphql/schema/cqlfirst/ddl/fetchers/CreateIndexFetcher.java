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
package io.stargate.graphql.schema.cqlfirst.ddl.fetchers;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.ImmutableCollectionIndexingType;

public class CreateIndexFetcher extends IndexFetcher {
  public CreateIndexFetcher(
      AuthorizationService authorizationService, DataStoreFactory dataStoreFactory) {
    super(authorizationService, dataStoreFactory, Scope.CREATE);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      String keyspaceName,
      String tableName) {
    String columnName = dataFetchingEnvironment.getArgument("columnName");

    String indexName = dataFetchingEnvironment.getArgument("indexName");

    boolean ifNotExists =
        dataFetchingEnvironment.getArgumentOrDefault("ifNotExists", Boolean.FALSE);
    String customIndexClass = dataFetchingEnvironment.getArgumentOrDefault("indexType", null);

    String indexKind = dataFetchingEnvironment.getArgumentOrDefault("indexKind", null);
    boolean indexKeys = false;
    boolean indexEntries = false;
    boolean indexValues = false;
    boolean indexFull = false;
    if (indexKind != null) {
      switch (indexKind) {
        case "KEYS":
          indexKeys = true;
          break;
        case "VALUES":
          indexValues = true;
          break;
        case "ENTRIES":
          indexEntries = true;
          break;
        case "FULL":
          indexFull = true;
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Invalid indexKind value: %s", indexKind));
      }
    }

    CollectionIndexingType indexingType =
        ImmutableCollectionIndexingType.builder()
            .indexEntries(indexEntries)
            .indexKeys(indexKeys)
            .indexValues(indexValues)
            .indexFull(indexFull)
            .build();

    return builder
        .create()
        .custom(customIndexClass)
        .index(indexName)
        .ifNotExists(ifNotExists)
        .on(keyspaceName, tableName)
        .column(columnName)
        .indexingType(indexingType)
        .build();
  }
}
