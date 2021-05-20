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
package io.stargate.graphql.web.resources;

import com.google.common.base.Throwables;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.graphql.schema.cqlfirst.SchemaFactory;
import io.stargate.graphql.schema.graphqlfirst.AdminSchemaBuilder;
import io.stargate.graphql.schema.graphqlfirst.migration.CassandraMigrator;
import io.stargate.graphql.schema.graphqlfirst.processor.ProcessedSchema;
import io.stargate.graphql.schema.graphqlfirst.processor.SchemaProcessor;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the {@link GraphQL} instances used by our REST resources.
 *
 * <p>This includes staying up to date with CQL schema changes.
 */
public class GraphqlCache implements KeyspaceChangeListener {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlCache.class);
  private static final boolean DISABLE_DEFAULT_KEYSPACE =
      Boolean.getBoolean("stargate.graphql.default_keyspace.disabled");

  private final Persistence persistence;
  private final DataStoreFactory dataStoreFactory;
  private final boolean enableGraphqlFirst;

  private final GraphQL ddlGraphql;
  private final GraphQL schemaFirstAdminGraphql;
  private final String defaultKeyspace;
  private final ConcurrentMap<String, DmlGraphqlHolder> dmlGraphqls = new ConcurrentHashMap<>();

  public GraphqlCache(
      Persistence persistence, DataStoreFactory dataStoreFactory, boolean enableGraphqlFirst) {
    this.persistence = persistence;
    this.dataStoreFactory = dataStoreFactory;
    this.enableGraphqlFirst = enableGraphqlFirst;

    this.ddlGraphql = newGraphql(SchemaFactory.newDdlSchema());
    this.schemaFirstAdminGraphql = GraphQL.newGraphQL(new AdminSchemaBuilder(this).build()).build();
    this.defaultKeyspace = findDefaultKeyspace(dataStoreFactory.createInternal());

    persistence.registerEventListener(this);
  }

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public GraphQL getSchemaFirstAdminGraphql() {
    return schemaFirstAdminGraphql;
  }

  public GraphQL getDml(
      String keyspaceName, AuthenticationSubject subject, Map<String, String> headers)
      throws Exception {

    DataStore dataStore = buildUserDatastore(subject, headers);
    SchemaSource latestSource =
        enableGraphqlFirst ? new SchemaSourceDao(dataStore).getLatestVersion(keyspaceName) : null;

    String decoratedKeyspaceName = persistence.decorateKeyspaceName(keyspaceName, headers);
    DmlGraphqlHolder currentHolder = dmlGraphqls.get(decoratedKeyspaceName);
    if (currentHolder != null && currentHolder.isSameVersionAs(latestSource)) {
      LOG.trace("Returning cached schema for {}", decoratedKeyspaceName);
      return currentHolder.getGraphql();
    }

    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      LOG.trace("Keyspace {} does not exist", decoratedKeyspaceName);
      return null;
    }

    LOG.trace(
        "Computing new version for {} ({})",
        decoratedKeyspaceName,
        (currentHolder == null) ? "wasn't cached before" : "schema has changed");
    DmlGraphqlHolder newHolder = new DmlGraphqlHolder(latestSource, keyspace);
    boolean installed =
        (currentHolder == null)
            ? dmlGraphqls.putIfAbsent(decoratedKeyspaceName, newHolder) == null
            : dmlGraphqls.replace(decoratedKeyspaceName, currentHolder, newHolder);

    if (installed) {
      LOG.trace("Installing new version for {} in the cache", decoratedKeyspaceName);
      newHolder.init();
      return newHolder.getGraphql();
    }

    LOG.trace(
        "Got beat installing new version for {} in the cache, fetching again",
        decoratedKeyspaceName);
    currentHolder = dmlGraphqls.get(decoratedKeyspaceName);
    return currentHolder == null ? null : currentHolder.getGraphql();
  }

  public void putDml(
      String keyspaceName, SchemaSource newSource, GraphQL graphql, AuthenticationSubject subject) {
    Map<String, String> headers = subject.customProperties();
    DataStore dataStore = buildUserDatastore(subject, headers);
    String decoratedKeyspaceName = persistence.decorateKeyspaceName(keyspaceName, headers);

    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);
    if (keyspace == null) {
      LOG.trace("Keyspace {} does not exist", decoratedKeyspaceName);
      return;
    }
    LOG.trace(
        "Putting new schema version: {} for {}", newSource.getVersion(), decoratedKeyspaceName);
    DmlGraphqlHolder schemaHolder = new DmlGraphqlHolder(newSource, graphql);
    dmlGraphqls.put(decoratedKeyspaceName, schemaHolder);
  }

  public String getDefaultKeyspaceName() {
    return defaultKeyspace;
  }

  private DataStore buildUserDatastore(AuthenticationSubject subject, Map<String, String> headers) {
    DataStoreOptions dataStoreOptions =
        DataStoreOptions.builder().putAllCustomProperties(headers).build();
    return dataStoreFactory.create(subject.asUser(), dataStoreOptions);
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
              .writeTimeColumn("durable_writes")
              .as("wt")
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

  @FormatMethod
  public void onKeyspaceChanged(
      String decoratedKeyspaceName, @FormatString String reason, Object... reasonArguments) {

    // CQL-first schemas react to CQL schema changes: invalidate the cached version so that it gets
    // regenerated.
    DmlGraphqlHolder holder = dmlGraphqls.get(decoratedKeyspaceName);
    if (holder != null
        && holder.isCqlFirst()
        && dmlGraphqls.remove(decoratedKeyspaceName) != null
        && LOG.isDebugEnabled()) {
      LOG.debug(
          "Invalidated GraphQL schema for keyspace {} because {}",
          decoratedKeyspaceName,
          String.format(reason, reasonArguments));
    }

    // Don't do anything for GraphQL-first schemas: we can't really accommodate external CQL
    // changes, since we don't control the GraphQL, the user does.
    // It is assumed that the data model will only evolve by deploying new GraphQL schema versions.
  }

  /**
   * Holds the GraphQL schema for a particular keyspace (either CQL-first or GraphQL-first,
   * depending on whether there is a SchemaSource for this keyspace).
   */
  class DmlGraphqlHolder {

    // the user-provided GraphQL schema, or null if CQL-first
    private final SchemaSource source;
    private final Keyspace keyspace;
    // This future is used to guarantee that the cached value is built only once, even if two
    // clients try to initialize it concurrently.
    private final CompletableFuture<GraphQL> graphqlFuture = new CompletableFuture<>();

    DmlGraphqlHolder(SchemaSource source, Keyspace keyspace) {
      this.source = source;
      this.keyspace = keyspace;
    }

    DmlGraphqlHolder(SchemaSource source, GraphQL graphQL) {
      this.source = source;
      this.graphqlFuture.complete(graphQL);
      // when the graphqlFuture is already completed, the keyspace is not-needed
      this.keyspace = null;
    }

    void init() {
      try {
        graphqlFuture.complete(buildGraphql());
      } catch (Exception e) {
        graphqlFuture.completeExceptionally(e);
      }
    }

    private GraphQL buildGraphql() {

      if (source == null) {
        return newGraphql(SchemaFactory.newDmlSchema(keyspace));
      } else {
        ProcessedSchema processedSchema =
            new SchemaProcessor(persistence, true).process(source.getContents(), keyspace);
        // Check that the data model still matches
        CassandraMigrator.forPersisted().compute(processedSchema.getMappingModel(), keyspace);

        return processedSchema.getGraphql();
      }
    }

    boolean isSameVersionAs(SchemaSource otherSource) {
      return (source == null && otherSource == null)
          || (source != null
              && otherSource != null
              && source.getVersion().equals(otherSource.getVersion()));
    }

    boolean isCqlFirst() {
      return source == null;
    }

    GraphQL getGraphql() throws Exception {
      try {
        return graphqlFuture.get();
      } catch (ExecutionException e) {
        Throwables.throwIfUnchecked(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }
  }
}
