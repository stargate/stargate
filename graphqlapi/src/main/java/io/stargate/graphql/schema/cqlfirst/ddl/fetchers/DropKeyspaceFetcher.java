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
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.graphql.web.StargateGraphqlContext;

public class DropKeyspaceFetcher extends DdlQueryFetcher {

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment environment, QueryBuilder builder, StargateGraphqlContext context)
      throws UnauthorizedException {
    String keyspaceName = environment.getArgument("name");

    context
        .getAuthorizationService()
        .authorizeSchemaWrite(
            context.getSubject(),
            keyspaceName,
            null,
            Scope.DROP,
            SourceAPI.GRAPHQL,
            ResourceKind.KEYSPACE);

    boolean ifExists = environment.getArgumentOrDefault("ifExists", Boolean.FALSE);

    return builder.drop().keyspace(keyspaceName).ifExists(ifExists).build();
  }
}
