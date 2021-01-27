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
package io.stargate.graphql.web;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.schema.SchemaFactory;
import java.util.Collections;
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
  private final AuthenticationService authenticationService;
  private final AuthorizationService authorizationService;
  private final DataStoreFactory dataStoreFactory;

  private final GraphQL ddlGraphql;
  private final String defaultKeyspace;
  private final ConcurrentMap<String, DmlGraphqlReference> dmlGraphqls;

  GraphqlCache(
      Persistence persistence,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    this.persistence = persistence;
    this.authenticationService = authenticationService;
    this.authorizationService = authorizationService;
    this.dataStoreFactory = dataStoreFactory;

    this.ddlGraphql =
        newGraphql(
            SchemaFactory.newDdlSchema(
                authenticationService, authorizationService, dataStoreFactory));
    DataStore dataStore = dataStoreFactory.createInternal();
    this.defaultKeyspace = findDefaultKeyspace(dataStore);
    this.dmlGraphqls =
        initDmlGraphqls(
            persistence, dataStore, authenticationService, authorizationService, dataStoreFactory);

    persistence.registerEventListener(this);
  }

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public GraphQL getDml(String keyspace, Map<String, String> headers) {
    String decoratedName = persistence.decorateKeyspaceName(keyspace, headers);
    DmlGraphqlReference ref = dmlGraphqls.get(decoratedName);
    return ref == null ? null : ref.get();
  }

  public GraphQL getDefaultDml() {
    return defaultKeyspace == null ? null : getDml(defaultKeyspace, Collections.emptyMap());
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

  private ConcurrentMap<String, DmlGraphqlReference> initDmlGraphqls(
      Persistence persistence,
      DataStore dataStore,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    ConcurrentMap<String, DmlGraphqlReference> map = new ConcurrentHashMap<>();

    for (Keyspace keyspace : dataStore.schema().keyspaces()) {
      String keyspaceName = keyspace.name();
      LOG.debug("Prepare GraphQL schema for {}", keyspaceName);
      map.put(
          keyspaceName,
          new DmlGraphqlReference(
              keyspace,
              persistence,
              authenticationService,
              authorizationService,
              dataStoreFactory));
    }
    return map;
  }

  @FormatMethod
  private void addOrReplaceDmlGraphql(
      String keyspaceName, @FormatString String reason, Object... reasonArguments) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Refreshing GraphQL schema for keyspace {} because {}",
          keyspaceName,
          String.format(reason, reasonArguments));
    }
    try {
      DataStore dataStore = dataStoreFactory.createInternal();
      Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
      if (keyspace == null) {
        // This happens when come from a notification for a keyspace that was just dropped
        LOG.debug("Removing GraphQL schema for keyspace {} because it was dropped", keyspaceName);
        dmlGraphqls.remove(keyspaceName);
      } else {
        dmlGraphqls.put(
            keyspaceName,
            new DmlGraphqlReference(
                keyspace,
                persistence,
                authenticationService,
                authorizationService,
                dataStoreFactory));
      }
      LOG.debug("Done refreshing GraphQL schema for keyspace {}", keyspaceName);
    } catch (Exception e) {
      LOG.error("Error while refreshing GraphQL schema for keyspace {}", keyspaceName, e);
    }
  }

  // Schema change callbacks: we refresh a keyspace whenever it gets created or dropped, or anything
  // inside it changes.
  // TODO maybe add debouncing mechanism to amortize quick event bursts

  @Override
  public void onCreateKeyspace(String keyspaceName) {
    addOrReplaceDmlGraphql(keyspaceName, "it was created");
  }

  @Override
  public void onDropKeyspace(String keyspaceName) {
    // Note that if the keyspace contained any children, we probably already removed the handler
    // while processing those children's DROP events.
    DmlGraphqlReference removed = dmlGraphqls.remove(keyspaceName);
    if (removed != null) {
      LOG.debug("Removing GraphQL schema for keyspace {} because it was dropped", keyspaceName);
    }
  }

  @Override
  public void onCreateTable(String keyspaceName, String table) {
    addOrReplaceDmlGraphql(keyspaceName, "table %s was created", table);
  }

  @Override
  public void onCreateView(String keyspaceName, String view) {
    addOrReplaceDmlGraphql(keyspaceName, "view %s was created", view);
  }

  @Override
  public void onCreateType(String keyspaceName, String type) {
    addOrReplaceDmlGraphql(keyspaceName, "type %s was created", type);
  }

  @Override
  public void onCreateFunction(String keyspaceName, String function, List<String> argumentTypes) {
    addOrReplaceDmlGraphql(keyspaceName, "function %s was created", function);
  }

  @Override
  public void onCreateAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    addOrReplaceDmlGraphql(keyspaceName, "aggregate %s was created", aggregate);
  }

  @Override
  public void onAlterTable(String keyspaceName, String table) {
    addOrReplaceDmlGraphql(keyspaceName, "table %s was altered", table);
  }

  @Override
  public void onAlterView(String keyspaceName, String view) {
    addOrReplaceDmlGraphql(keyspaceName, "view %s was altered", view);
  }

  @Override
  public void onAlterType(String keyspaceName, String type) {
    addOrReplaceDmlGraphql(keyspaceName, "type %s was altered", type);
  }

  @Override
  public void onAlterFunction(String keyspaceName, String function, List<String> argumentTypes) {
    addOrReplaceDmlGraphql(keyspaceName, "function %s was altered", function);
  }

  @Override
  public void onAlterAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    addOrReplaceDmlGraphql(keyspaceName, "aggregate %s was altered", aggregate);
  }

  @Override
  public void onDropTable(String keyspaceName, String table) {
    addOrReplaceDmlGraphql(keyspaceName, "table %s was dropped", table);
  }

  @Override
  public void onDropView(String keyspaceName, String view) {
    addOrReplaceDmlGraphql(keyspaceName, "view %s was dropped", view);
  }

  @Override
  public void onDropType(String keyspaceName, String type) {
    addOrReplaceDmlGraphql(keyspaceName, "type %s was dropped", type);
  }

  @Override
  public void onDropFunction(String keyspaceName, String function, List<String> argumentTypes) {
    addOrReplaceDmlGraphql(keyspaceName, "function %s was dropped", function);
  }

  @Override
  public void onDropAggregate(String keyspaceName, String aggregate, List<String> argumentTypes) {
    addOrReplaceDmlGraphql(keyspaceName, "aggregate %s was dropped", aggregate);
  }

  /**
   * Lazily holds the {@link GraphQL} instance for the DML operations on a particular keyspace: it
   * will only be initialized if the keyspace is queried.
   */
  static class DmlGraphqlReference {

    private final Keyspace keyspace;
    private final Persistence persistence;
    private final AuthenticationService authenticationService;
    private final AuthorizationService authorizationService;
    private final DataStoreFactory dataStoreFactory;

    private volatile GraphQL graphql;

    DmlGraphqlReference(
        Keyspace keyspace,
        Persistence persistence,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        DataStoreFactory dataStoreFactory) {
      this.keyspace = keyspace;
      this.persistence = persistence;
      this.authenticationService = authenticationService;
      this.authorizationService = authorizationService;
      this.dataStoreFactory = dataStoreFactory;
    }

    GraphQL get() {
      // Double-checked locking
      GraphQL result = graphql;
      if (result != null) {
        return result;
      }
      synchronized (this) {
        if (graphql == null) {
          graphql =
              newGraphql(
                  SchemaFactory.newDmlSchema(
                      authenticationService, authorizationService, keyspace, dataStoreFactory));
        }
        return graphql;
      }
    }
  }

  private static GraphQL newGraphql(GraphQLSchema schema) {
    return GraphQL.newGraphQL(schema)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(new AsyncExecutionStrategy())
        .build();
  }
}
