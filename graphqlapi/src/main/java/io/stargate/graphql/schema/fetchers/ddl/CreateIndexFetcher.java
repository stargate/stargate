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
package io.stargate.graphql.schema.fetchers.ddl;

import com.google.common.base.Splitter;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.CollectionIndexingType;
import io.stargate.db.schema.ImmutableCollectionIndexingType;
import java.util.Arrays;
import java.util.List;

public class CreateIndexFetcher extends DdlQueryFetcher {

  public CreateIndexFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment, QueryBuilder builder) {

    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String tableName = dataFetchingEnvironment.getArgument("tableName");
    String columnName = dataFetchingEnvironment.getArgument("columnName");

    if (keyspaceName == null) {
      List<String> parts = Splitter.on('.').splitToList(tableName);
      if (parts.size() < 2) {
        throw new IllegalArgumentException("Missing field argument keyspaceName @ 'createIndex'");
      }
      keyspaceName = parts.get(0);
      tableName = parts.get(1);
    }

    String indexName =
        dataFetchingEnvironment.getArgumentOrDefault(
            "indexName", tableName + "_" + columnName + "_idx");

    boolean ifNotExists =
        dataFetchingEnvironment.getArgumentOrDefault("ifNotExists", Boolean.FALSE);
    String customIndexClass =
        dataFetchingEnvironment.getArgumentOrDefault("customIndexClass", null);

    boolean indexEntries =
        dataFetchingEnvironment.getArgumentOrDefault("indexEntries", Boolean.FALSE);
    boolean indexKeys = dataFetchingEnvironment.getArgumentOrDefault("indexKeys", Boolean.FALSE);
    boolean indexValues =
        dataFetchingEnvironment.getArgumentOrDefault("indexValues", Boolean.FALSE);
    boolean indexFull = dataFetchingEnvironment.getArgumentOrDefault("indexFull", Boolean.FALSE);

    long paramsCount =
        Arrays.asList(indexEntries, indexKeys, indexValues, indexFull).stream()
            .filter(i -> i)
            .count();

    if (paramsCount > 1) {
      throw new IllegalArgumentException(
          "The indexEntries, indexKeys, indexValues and indexFull options are mutually exclusive");
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
