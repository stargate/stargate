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
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.UserDefinedType;

public class DropTypeFetcher extends DdlQueryFetcher {

  public DropTypeFetcher(
      AuthorizationService authorizationService, DataStoreFactory dataStoreFactory) {
    super(authorizationService, dataStoreFactory);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {

    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String typeName = dataFetchingEnvironment.getArgument("typeName");

    // Permissions on a type are the same as keyspace
    authorizationService.authorizeSchemaWrite(
        authenticationSubject, keyspaceName, null, Scope.DROP, SourceAPI.GRAPHQL);

    Boolean ifExists = dataFetchingEnvironment.getArgument("ifExists");
    return builder
        .drop()
        .type(keyspaceName, UserDefinedType.reference(typeName))
        .ifExists(ifExists != null && ifExists)
        .build();
  }
}
