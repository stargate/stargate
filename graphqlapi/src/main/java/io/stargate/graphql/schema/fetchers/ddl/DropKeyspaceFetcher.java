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

import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.*;
import io.stargate.db.Persistence;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.graphql.web.HttpAwareContext;

public class DropKeyspaceFetcher extends DdlQueryFetcher {

  public DropKeyspaceFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment, QueryBuilder builder)
      throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("name");

    HttpAwareContext httpAwareContext = dataFetchingEnvironment.getContext();
    String token = httpAwareContext.getAuthToken();
    authorizationService.authorizeSchemaWrite(
        token, keyspaceName, null, Scope.DROP, SourceAPI.GRAPHQL);

    boolean ifExists = dataFetchingEnvironment.getArgumentOrDefault("ifExists", Boolean.FALSE);

    return builder.drop().keyspace(keyspaceName).ifExists(ifExists).build();
  }
}
