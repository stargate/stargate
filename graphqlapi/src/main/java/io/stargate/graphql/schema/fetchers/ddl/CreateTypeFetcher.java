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
import io.stargate.auth.AuthenticationPrincipal;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTypeFetcher extends DdlQueryFetcher {

  public CreateTypeFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment dataFetchingEnvironment,
      QueryBuilder builder,
      AuthenticationPrincipal authenticationPrincipal)
      throws UnauthorizedException {

    String keyspaceName = dataFetchingEnvironment.getArgument("keyspaceName");
    String typeName = dataFetchingEnvironment.getArgument("typeName");

    // Permissions on a type are the same as keyspace
    authorizationService.authorizeSchemaWrite(
        authenticationPrincipal, keyspaceName, null, Scope.CREATE, SourceAPI.GRAPHQL);

    List<Map<String, Object>> fieldList = dataFetchingEnvironment.getArgument("fields");
    if (fieldList.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one field");
    }
    List<Column> fields = new ArrayList<>(fieldList.size());
    for (Map<String, Object> key : fieldList) {
      fields.add(Column.create((String) key.get("name"), decodeType(key.get("type"))));
    }
    UserDefinedType udt =
        ImmutableUserDefinedType.builder()
            .keyspace(keyspaceName)
            .name(typeName)
            .addAllColumns(fields)
            .build();

    Boolean ifNotExists = dataFetchingEnvironment.getArgument("ifNotExists");
    return builder
        .create()
        .type(keyspaceName, udt)
        .ifNotExists(ifNotExists != null && ifNotExists)
        .build();
  }
}
