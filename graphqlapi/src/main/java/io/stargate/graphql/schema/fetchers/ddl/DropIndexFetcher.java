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
import java.util.List;

public class DropIndexFetcher extends DdlQueryFetcher {

  public DropIndexFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment, QueryBuilder builder) {

    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String indexName = dataFetchingEnvironment.getArgument("indexName");
    boolean ifExists = dataFetchingEnvironment.getArgumentOrDefault("ifExists", Boolean.FALSE);

    if (keyspaceName == null) {
      List<String> parts = Splitter.on('.').splitToList(indexName);
      if (parts.size() < 2) {
        throw new IllegalArgumentException("Missing field argument keyspaceName @ 'dropIndex'");
      }
      keyspaceName = parts.get(0);
      indexName = parts.get(1);
    }

    return builder.drop().index(keyspaceName, indexName).ifExists(ifExists).build();
  }
}
