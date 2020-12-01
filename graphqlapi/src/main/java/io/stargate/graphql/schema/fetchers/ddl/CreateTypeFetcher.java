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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTypeStart;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Persistence;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.List;
import java.util.Map;

public class CreateTypeFetcher extends DdlQueryFetcher {

  public CreateTypeFetcher(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(persistence, authenticationService, authorizationService);
  }

  @Override
  String getQuery(DataFetchingEnvironment dataFetchingEnvironment) throws UnauthorizedException {
    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String typeName = dataFetchingEnvironment.getArgument("typeName");

    HttpAwareContext httpAwareContext = dataFetchingEnvironment.getContext();
    String token = httpAwareContext.getAuthToken();
    // Permissions on a type are the same as keyspace
    authorizationService.authorizeSchemaWrite(token, keyspaceName, null, Scope.CREATE);

    CreateTypeStart start =
        SchemaBuilder.createType(
            CqlIdentifier.fromInternal(keyspaceName), CqlIdentifier.fromInternal(typeName));
    Boolean ifNotExists = dataFetchingEnvironment.getArgument("ifNotExists");
    if (ifNotExists != null && ifNotExists) {
      start = start.ifNotExists();
    }
    CreateType createType = null;
    List<Map<String, Object>> field = dataFetchingEnvironment.getArgument("fields");
    if (field.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one field");
    }
    for (Map<String, Object> key : field) {
      createType =
          (createType == null ? start : createType)
              .withField(
                  CqlIdentifier.fromInternal((String) key.get("name")),
                  decodeType(key.get("type")));
    }
    return createType.asCql();
  }
}
