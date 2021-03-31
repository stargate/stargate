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
package io.stargate.graphql.web.resources.cqlfirst;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.cqlfirst.SchemaFactory;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the {@link GraphQL} instances used by our REST resources.
 *
 * <p>This includes staying up to date with CQL schema changes.
 */
public class GraphqlCache implements EventListener {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlCache.class);
  private static final boolean DISABLE_DEFAULT_KEYSPACE =
      Boolean.getBoolean("stargate.graphql.default_keyspace.disabled");

  private final Persistence persistence;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;

  private final GraphQL ddlGraphql;
  private final String defaultKeyspace;
  private final ConcurrentMap<String, GraphQL> dmlGraphqls = new ConcurrentHashMap<>();

  public GraphqlCache(
      Persistence persistence,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this.persistence = persistence;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;

    this.ddlGraphql =
        newGraphql(SchemaFactory.newDdlSchema(authorizationService, dataStoreFactory));
    this.defaultKeyspace = findDefaultKeyspace(dataStoreFactory.createInternal());

    persistence.registerEventListener(this);
  }

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public GraphQL getDml(
      String keyspaceName, AuthenticationSubject subject, Map<String, String> headers) {
    String decoratedKeyspaceName = persistence.decorateKeyspaceName(keyspaceName, headers);
    return dmlGraphqls.computeIfAbsent(
        decoratedKeyspaceName, __ -> buildDmlGraphql(keyspaceName, subject, headers));
  }

  public GraphQL getDefaultDml(Map<String, String> headers) {
    return defaultKeyspace == null ? null : getDml(defaultKeyspace, null, headers);
  }

  public String getDefaultKeyspaceName() {
    return defaultKeyspace;
  }

  private GraphQL buildDmlGraphql(
      String keyspaceName, AuthenticationSubject subject, Map<String, String> headers) {
    DataStoreOptions dataStoreOptions =
        DataStoreOptions.builder().putAllCustomProperties(headers).build();
    DataStore dataStore =
        subject == null
            ? dataStoreFactory.createInternal(dataStoreOptions)
            : dataStoreFactory.create(subject.asUser(), dataStoreOptions);
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    return (keyspace == null)
        ? null
        : newGraphql(SchemaFactory.newDmlSchema(authorizationService, keyspace, dataStoreFactory));
  }

  private static GraphQL newGraphql(GraphQLSchema schema) {
    return GraphQL.newGraphQL(schema)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(new AsyncExecutionStrategy())
        .build();
  }

  /** Populate a default keyspace to allow for omitting the keyspace from the path of requests. */
  private static String findDefaultKeyspace(DataStore dataStore) {
    if (DISABLE_DEFAULT_KEYSPACE) return null;

    try {
      CompletableFuture<ResultSet> query =
          dataStore
              .queryBuilder()
              .select()
              .column("keyspace_name")
              .writeTimeColumn("durable_writes", "wt")
              .from("system_schema", "keyspaces")
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM);

      ResultSet resultSet = query.get();

      // Grab the oldest, non-system keyspace to use as default.
      Optional<Row> first =
          resultSet.rows().stream()
              .filter(r -> !r.isNull("wt"))
              .filter(r -> r.getLong("wt") > 0)
              .filter(
                  r -> {
                    String keyspaceName = r.getString("keyspace_name");
                    return !keyspaceName.equals("system")
                        && !keyspaceName.equals("data_endpoint_auth")
                        && !keyspaceName.equals("solr_admin")
                        && !keyspaceName.startsWith("system_")
                        && !keyspaceName.startsWith("dse_");
                  })
              .min(Comparator.comparing(r -> r.getLong("wt")));

      String defaultKeyspace = first.map(row -> row.getString("keyspace_name")).orElse(null);
      LOG.debug("Using default keyspace {}", defaultKeyspace);
      return defaultKeyspace;
    } catch (Exception e) {
      LOG.warn("Unable to get default keyspace", e);
      return null;
    }
  }

  // Schema change callbacks: whenever something changes in the keyspace, we invalidate the
  // corresponding GraphQL schema. The next client request will recompute it.
  // TODO maybe add debouncing mechanism to amortize quick event bursts

  @Override
  public void onDropKeyspace(String decoratedKeyspaceName) {
    invalidateDmlGraphql(decoratedKeyspaceName, "it was dropped");
  }

  @Override
  public void onCreateTable(String decoratedKeyspaceName, String table) {
    invalidateDmlGraphql(decoratedKeyspaceName, "table %s was created", table);
  }

  @Override
  public void onCreateView(String decoratedKeyspaceName, String view) {
    invalidateDmlGraphql(decoratedKeyspaceName, "view %s was created", view);
  }

  @Override
  public void onCreateType(String decoratedKeyspaceName, String type) {
    invalidateDmlGraphql(decoratedKeyspaceName, "type %s was created", type);
  }

  @Override
  public void onCreateFunction(
      String decoratedKeyspaceName, String function, List<String> argumentTypes) {
    invalidateDmlGraphql(decoratedKeyspaceName, "function %s was created", function);
  }

  @Override
  public void onCreateAggregate(
      String decoratedKeyspaceName, String aggregate, List<String> argumentTypes) {
    invalidateDmlGraphql(decoratedKeyspaceName, "aggregate %s was created", aggregate);
  }

  @Override
  public void onAlterTable(String decoratedKeyspaceName, String table) {
    invalidateDmlGraphql(decoratedKeyspaceName, "table %s was altered", table);
  }

  @Override
  public void onAlterView(String decoratedKeyspaceName, String view) {
    invalidateDmlGraphql(decoratedKeyspaceName, "view %s was altered", view);
  }

  @Override
  public void onAlterType(String decoratedKeyspaceName, String type) {
    invalidateDmlGraphql(decoratedKeyspaceName, "type %s was altered", type);
  }

  @Override
  public void onAlterFunction(
      String decoratedKeyspaceName, String function, List<String> argumentTypes) {
    invalidateDmlGraphql(decoratedKeyspaceName, "function %s was altered", function);
  }

  @Override
  public void onAlterAggregate(
      String decoratedKeyspaceName, String aggregate, List<String> argumentTypes) {
    invalidateDmlGraphql(decoratedKeyspaceName, "aggregate %s was altered", aggregate);
  }

  @Override
  public void onDropTable(String decoratedKeyspaceName, String table) {
    invalidateDmlGraphql(decoratedKeyspaceName, "table %s was dropped", table);
  }

  @Override
  public void onDropView(String decoratedKeyspaceName, String view) {
    invalidateDmlGraphql(decoratedKeyspaceName, "view %s was dropped", view);
  }

  @Override
  public void onDropType(String decoratedKeyspaceName, String type) {
    invalidateDmlGraphql(decoratedKeyspaceName, "type %s was dropped", type);
  }

  @Override
  public void onDropFunction(
      String decoratedKeyspaceName, String function, List<String> argumentTypes) {
    invalidateDmlGraphql(decoratedKeyspaceName, "function %s was dropped", function);
  }

  @Override
  public void onDropAggregate(
      String decoratedKeyspaceName, String aggregate, List<String> argumentTypes) {
    invalidateDmlGraphql(decoratedKeyspaceName, "aggregate %s was dropped", aggregate);
  }

  @FormatMethod
  private void invalidateDmlGraphql(
      String decoratedKeyspaceName, @FormatString String reason, Object... reasonArguments) {
    GraphQL previous = dmlGraphqls.remove(decoratedKeyspaceName);
    if (previous != null && LOG.isDebugEnabled()) {
      LOG.debug(
          "Invalidated GraphQL schema for keyspace {} because {}",
          decoratedKeyspaceName,
          String.format(reason, reasonArguments));
    }
  }
}
