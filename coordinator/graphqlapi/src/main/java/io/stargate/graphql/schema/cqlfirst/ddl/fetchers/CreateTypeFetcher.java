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
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateTypeFetcher extends DdlQueryFetcher {

  @Override
  protected Query<?> buildQuery(
      DataFetchingEnvironment environment, QueryBuilder builder, StargateGraphqlContext context)
      throws UnauthorizedException {

    String keyspaceName = environment.getArgument("keyspaceName");
    String typeName = environment.getArgument("typeName");

    // Permissions on a type are the same as keyspace
    context
        .getAuthorizationService()
        .authorizeSchemaWrite(
            context.getSubject(),
            keyspaceName,
            null,
            Scope.CREATE,
            SourceAPI.GRAPHQL,
            ResourceKind.TYPE);

    List<Map<String, Object>> fieldList = environment.getArgument("fields");
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

    Boolean ifNotExists = environment.getArgument("ifNotExists");
    return builder
        .create()
        .type(keyspaceName, udt)
        .ifNotExists(ifNotExists != null && ifNotExists)
        .build();
  }
}
