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
package io.stargate.graphql.schema.schemafirst.fetchers;

import com.google.common.collect.ImmutableMap;
import graphql.ExceptionWhileDataFetching;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.query.builder.Replication;
import io.stargate.db.schema.Keyspace;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;

public class CreateNamespaceFetcher
    extends NamespaceFetcher<DataFetcherResult<Map<String, Object>>> {

  public CreateNamespaceFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  @Override
  protected DataFetcherResult<Map<String, Object>> get(
      DataFetchingEnvironment environment,
      DataStore dataStore,
      AuthenticationSubject authenticationSubject) {

    String name = environment.getArgument("name");

    try {
      authorizationService.authorizeSchemaWrite(
          authenticationSubject, name, null, Scope.CREATE, SourceAPI.GRAPHQL);
    } catch (UnauthorizedException e) {
      return errorResult(e, environment).build();
    }

    boolean ifNotExists = environment.getArgumentOrDefault("ifNotExists", Boolean.FALSE);
    Integer replicas = environment.getArgument("replicas");
    List<Map<String, Object>> datacenters = environment.getArgument("datacenters");
    if (replicas == null && datacenters == null) {
      replicas = 1;
    }
    if (replicas != null && datacenters != null) {
      throw new IllegalArgumentException("You can't specify both replicas and datacenters");
    }
    Replication replication =
        replicas != null
            ? Replication.simpleStrategy(replicas)
            : Replication.networkTopologyStrategy(parseDatacenters(datacenters));

    DataFetcherResult.Builder<Map<String, Object>> result;
    try {
      dataStore
          .execute(
              dataStore
                  .queryBuilder()
                  .create()
                  .keyspace(name)
                  .ifNotExists(ifNotExists)
                  .withReplication(replication)
                  .build()
                  .bind())
          .get();
      result = DataFetcherResult.newResult();
    } catch (Exception e) {
      if (e instanceof ExecutionException && e.getCause() instanceof AlreadyExistsException) {
        // Error out, but we'll still include the details of the existing keyspace below.
        result = errorResult(e.getCause(), environment);
      } else {
        // Unexpected technical error: rethrow directly
        return errorResult(e, environment).build();
      }
    }

    ImmutableMap.Builder<String, Object> data = ImmutableMap.builder();
    data.put("query", environment.getGraphQLSchema().getQueryType());

    Keyspace keyspace = dataStore.schema().keyspace(name);
    if (isReadAuthorized(keyspace, authenticationSubject)) {
      data.put("namespace", formatNamespace(keyspace));
    }
    return result.data(data.build()).build();
  }

  private DataFetcherResult.Builder<Map<String, Object>> errorResult(
      Throwable t, DataFetchingEnvironment environment) {
    return DataFetcherResult.<Map<String, Object>>newResult()
        .error(
            new ExceptionWhileDataFetching(
                environment.getExecutionStepInfo().getPath(),
                t,
                environment.getMergedField().getSingleField().getSourceLocation()));
  }

  private Map<String, Integer> parseDatacenters(List<Map<String, Object>> datacenters) {
    assert datacenters != null;
    if (datacenters.isEmpty()) {
      throw new IllegalArgumentException("datacenters must contain at least one element");
    }
    Map<String, Integer> result = new HashMap<>();
    for (Map<String, Object> datacenter : datacenters) {
      String dcName = (String) datacenter.get("name");
      Integer dcReplicas = (Integer) datacenter.getOrDefault("replicas", 3);
      result.put(dcName, dcReplicas);
    }
    return result;
  }
}
