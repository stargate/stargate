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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.DefaultRaw;
import com.datastax.oss.driver.internal.querybuilder.term.TupleTerm;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.TypedKeyValue;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import io.stargate.graphql.web.HttpAwareContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InsertMutationFetcher extends MutationFetcher {

  public InsertMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService) {
    super(table, nameMapping, persistence, authenticationService, authorizationService);
  }

  @Override
  protected String buildStatement(DataFetchingEnvironment environment, DataStore dataStore)
      throws Exception {
    Map<CqlIdentifier, Term> cqlIdentifierTermMap = buildInsertValues(environment);

    HttpAwareContext httpAwareContext = environment.getContext();
    String token = httpAwareContext.getAuthToken();

    authorizationService.authorizeDataWrite(
        token, buildTypedKeyValueList(cqlIdentifierTermMap), Scope.MODIFY);

    Insert insert =
        QueryBuilder.insertInto(keyspaceId, tableId).valuesByIds(buildInsertValues(environment));

    if (environment.containsArgument("ifNotExists")
        && environment.getArgument("ifNotExists") != null
        && (Boolean) environment.getArgument("ifNotExists")) {
      insert = insert.ifNotExists();
    }
    if (environment.containsArgument("options") && environment.getArgument("options") != null) {
      Map<String, Object> options = environment.getArgument("options");
      if (options.containsKey("ttl") && options.get("ttl") != null) {
        insert = insert.usingTtl((Integer) options.get("ttl"));
      }
    }

    return insert.asCql();
  }

  private List<TypedKeyValue> buildTypedKeyValueList(
      Map<CqlIdentifier, Term> cqlIdentifierTermMap) {
    List<TypedKeyValue> typedKeyValues = new ArrayList<>();
    for (Map.Entry<CqlIdentifier, Term> entry : cqlIdentifierTermMap.entrySet()) {
      CqlIdentifier key = entry.getKey();
      Term term = entry.getValue();
      Column column = getColumn(table, key.asInternal());
      ColumnType columnType = Objects.requireNonNull(column.type());

      if (isOrContainsUDT(columnType)) {
        // Null out the value for now since UDTs are not allowed for use with custom authorization
        typedKeyValues.add(new TypedKeyValue(column.cqlName(), columnType.cqlDefinition(), null));
        continue;
      }

      if (term instanceof TupleTerm) {
        for (Term component : ((TupleTerm) term).getComponents()) {
          Object parsedObject =
              columnType.codec().parse(((DefaultRaw) component).getRawExpression());

          typedKeyValues.add(
              new TypedKeyValue(column.cqlName(), columnType.cqlDefinition(), parsedObject));
        }
      } else {
        Object parsedObject = columnType.codec().parse(((DefaultRaw) term).getRawExpression());

        typedKeyValues.add(
            new TypedKeyValue(column.cqlName(), columnType.cqlDefinition(), parsedObject));
      }
    }

    return typedKeyValues;
  }

  private Map<CqlIdentifier, Term> buildInsertValues(DataFetchingEnvironment environment) {
    Map<String, Object> value = environment.getArgument("value");
    Preconditions.checkNotNull(value, "Insert statement must contain at least one field");

    Map<CqlIdentifier, Term> insertMap = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : value.entrySet()) {
      Column column = getColumn(table, entry.getKey());
      insertMap.put(CqlIdentifier.fromInternal(column.name()), toCqlTerm(column, entry.getValue()));
    }
    return insertMap;
  }
}
