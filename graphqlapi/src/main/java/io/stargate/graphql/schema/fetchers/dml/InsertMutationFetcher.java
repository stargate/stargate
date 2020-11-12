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
import com.google.common.base.Preconditions;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.NameMapping;
import java.util.LinkedHashMap;
import java.util.Map;

public class InsertMutationFetcher extends MutationFetcher {

  public InsertMutationFetcher(
      Table table,
      NameMapping nameMapping,
      Persistence persistence,
      AuthenticationService authenticationService) {
    super(table, nameMapping, persistence, authenticationService);
  }

  @Override
  protected String buildStatement(DataFetchingEnvironment environment, DataStore dataStore) {
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
