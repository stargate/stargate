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
package io.stargate.graphql.schema.graphqlfirst.fetchers.admin;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.datastore.DataStore;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.Collections;

public abstract class SchemaFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected void authorize(StargateGraphqlContext context, String keyspace)
      throws UnauthorizedException {
    context
        .getAuthorizationService()
        .authorizeSchemaRead(
            context.getSubject(),
            Collections.singletonList(SchemaSourceDao.KEYSPACE_NAME),
            Collections.singletonList(SchemaSourceDao.TABLE_NAME),
            SourceAPI.GRAPHQL,
            ResourceKind.TABLE);

    // Also check that the user has access to the keyspace. We don't want to let them see the
    // GraphQL schema for something that's forbidden to them.
    context
        .getAuthorizationService()
        .authorizeSchemaRead(
            context.getSubject(),
            Collections.singletonList(keyspace),
            Collections.emptyList(),
            SourceAPI.GRAPHQL,
            ResourceKind.KEYSPACE);
  }

  protected String getKeyspace(DataFetchingEnvironment environment, DataStore dataStore) {
    String keyspace = environment.getArgument("keyspace");
    if (!dataStore.schema().keyspaceNames().contains(keyspace)) {
      throw new IllegalArgumentException(String.format("Keyspace '%s' does not exist.", keyspace));
    }
    return keyspace;
  }
}
