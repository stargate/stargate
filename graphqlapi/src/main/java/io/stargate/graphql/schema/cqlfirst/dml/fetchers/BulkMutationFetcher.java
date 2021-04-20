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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static io.stargate.graphql.schema.SchemaConstants.ATOMIC_DIRECTIVE;

import com.google.common.collect.ImmutableMap;
import graphql.language.OperationDefinition;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.schema.Table;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class BulkMutationFetcher
    extends DmlFetcher<List<CompletableFuture<Map<String, Object>>>> {
  protected BulkMutationFetcher(
      Table table,
      NameMapping nameMapping,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(table, nameMapping, authorizationService, dataStoreFactory);
  }

  @Override
  protected List<CompletableFuture<Map<String, Object>>> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject) {
    List<BoundQuery> queries = null;
    Exception buildException = null;

    // Avoid mixing sync and async exceptions
    try {
      // buildStatement() could throw an unchecked exception.
      // As the statement might be part of a batch, we need to make sure the
      // batched operation completes.
      queries = buildQuery(environment, dataStore, authenticationSubject);
    } catch (Exception e) {
      buildException = e;
    }

    if (buildException != null) {
      CompletableFuture<Map<String, Object>> f = new CompletableFuture<>();
      f.completeExceptionally(buildException);
      return Collections.singletonList(f);
    }

    OperationDefinition operation = environment.getOperationDefinition();
    List<Map<String, Object>> values = environment.getArgument("values");
    if (values.size() != queries.size()) {
      throw new IllegalStateException("Number of values to insert should match number of queries");
    }

    List<CompletableFuture<Map<String, Object>>> results = new ArrayList<>(values.size());
    for (int i = 0; i < queries.size(); i++) {
      if (operation.getDirectives().stream().anyMatch(d -> d.getName().equals(ATOMIC_DIRECTIVE))
          && operation.getSelectionSet().getSelections().size() > 1) {
        throw new UnsupportedOperationException(
            "The @" + ATOMIC_DIRECTIVE + "in not supported for bulk inserts.");
      }

      // Execute as a single statement
      int finalI = i;
      results.add(
          dataStore
              .execute(queries.get(i))
              .thenApply(rs -> ImmutableMap.of("value", values.get(finalI))));
    }
    return results;
  }

  protected abstract List<BoundQuery> buildQuery(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject)
      throws Exception;
}
