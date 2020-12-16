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
package io.stargate.graphql.schema.fetchers.dml;

import com.google.common.base.Preconditions;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.*;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InsertMutationFetcher extends MutationFetcher {

  public InsertMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(
        table,
        nameMapping,
        persistence,
        authenticationService,
        authorizationService,
        dataStoreFactory);
  }

  @Override
  protected BoundQuery buildQuery(DataFetchingEnvironment environment, DataStore dataStore)
      throws UnauthorizedException {
    boolean ifNotExists =
        environment.containsArgument("ifNotExists")
            && environment.getArgument("ifNotExists") != null
            && (Boolean) environment.getArgument("ifNotExists");

    BoundQuery query =
        dataStore
            .queryBuilder()
            .insertInto(table.keyspace(), table.name())
            .value(buildInsertValues(environment))
            .ifNotExists(ifNotExists)
            .ttl(getTTL(environment))
            .build()
            .bind();

    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    authorizationService.authorizeDataWrite(
        token,
        table.keyspace(),
        table.name(),
        TypedKeyValue.forDML((BoundDMLQuery) query),
        Scope.MODIFY,
        SourceAPI.GRAPHQL);

    return query;
  }

  private List<ValueModifier> buildInsertValues(DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    Preconditions.checkNotNull(value, "Insert statement must contain at least one field");

    List<ValueModifier> modifiers = new ArrayList<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = getColumn(table, entry.getKey());
      modifiers.add(ValueModifier.set(column.name(), toDBValue(column, entry.getValue())));
    }
    return modifiers;
  }
}
