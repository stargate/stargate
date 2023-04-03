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

import com.datastax.oss.driver.shaded.guava.common.base.Supplier;
import com.datastax.oss.driver.shaded.guava.common.base.Suppliers;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import edu.umd.cs.findbugs.annotations.Nullable;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Keyspace;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.graphql.schema.CassandraFetcherExceptionHandler;
import io.stargate.graphql.schema.cqlfirst.SchemaFactory;
import io.stargate.graphql.schema.graphqlfirst.AdminSchemaBuilder;
import io.stargate.graphql.schema.graphqlfirst.migration.CassandraMigrator;
import io.stargate.graphql.schema.graphqlfirst.processor.ProcessedSchema;
import io.stargate.graphql.schema.graphqlfirst.processor.SchemaProcessor;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
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
public class GraphqlCache implements KeyspaceChangeListener {

  private static final Logger LOG = LoggerFactory.getLogger(GraphqlCache.class);
  private static final boolean DISABLE_DEFAULT_KEYSPACE =
      Boolean.getBoolean("stargate.graphql.default_keyspace.disabled");

  private final Persistence persistence;
  private final boolean enableGraphqlFirst;

  private final GraphQL ddlGraphql;
  private final GraphQL schemaFirstAdminGraphql;
  private final String defaultKeyspace;
  private final ConcurrentMap<String, GraphqlHolder> dmlGraphqls = new ConcurrentHashMap<>();

  public GraphqlCache(
      Persistence persistence, DataStoreFactory dataStoreFactory, boolean enableGraphqlFirst) {
    this.persistence = persistence;
    this.enableGraphqlFirst = enableGraphqlFirst;

    this.ddlGraphql = newGraphql(SchemaFactory.newDdlSchema());
    this.schemaFirstAdminGraphql = newGraphql(new AdminSchemaBuilder().build());
    this.defaultKeyspace = findDefaultKeyspace(dataStoreFactory.createInternal());

    persistence.registerEventListener(this);
  }

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public GraphQL getSchemaFirstAdminGraphql() {
    return schemaFirstAdminGraphql;
  }

  public GraphQL getDml(String keyspaceName, DataStore dataStore, Map<String, String> headers)
      throws Exception {

    SchemaSource latestSource =
        enableGraphqlFirst ? new SchemaSourceDao(dataStore).getLatestVersion(keyspaceName) : null;

    String decoratedKeyspaceName = persistence.decorateKeyspaceName(keyspaceName, headers);
    final GraphqlHolder currentHolder = dmlGraphqls.get(decoratedKeyspaceName);
    if (currentHolder != null && currentHolder.matches(latestSource)) {
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
    GraphqlHolder newHolder =
        (latestSource == null)
            ? new LazyCqlFirstGraphqlHolder(keyspace)
            : new LazySchemaFirstGraphqlHolder(latestSource, keyspace);

    // Put with a CAS, in case someone else deployed the new version before us:
    GraphqlHolder result =
        dmlGraphqls.compute(
            decoratedKeyspaceName, (__, v) -> Objects.equals(v, currentHolder) ? newHolder : v);
    return result == null ? null : result.getGraphql();
  }

  public void putDml(
      String keyspaceName, SchemaSource newSource, GraphQL graphql, AuthenticationSubject subject) {
    String decoratedKeyspaceName =
        persistence.decorateKeyspaceName(keyspaceName, subject.customProperties());

    LOG.trace(
        "Putting new schema version: {} for {}", newSource.getVersion(), decoratedKeyspaceName);
    GraphqlHolder schemaHolder = new ImmediateSchemaFirstGraphqlHolder(newSource, graphql);
    dmlGraphqls.put(decoratedKeyspaceName, schemaHolder);
  }

  public String getDefaultKeyspaceName() {
    return defaultKeyspace;
  }

  private static GraphQL newGraphql(GraphQLSchema schema) {
    return GraphQL.newGraphQL(schema)
        .defaultDataFetcherExceptionHandler(CassandraFetcherExceptionHandler.INSTANCE)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(
            new AsyncExecutionStrategy(CassandraFetcherExceptionHandler.INSTANCE))
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
    GraphqlHolder holder = dmlGraphqls.get(decoratedKeyspaceName);
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
   * An entry that holds the GraphQL schema cached for a particular keyspace (either CQL-first or
   * GraphQL-first, depending on whether a custom schema was deployed).
   */
  interface GraphqlHolder {
    GraphQL getGraphql();

    /**
     * Whether the schema matches the given source (if it doesn't, it will have to be recomputed).
     * Note that the source can be {@code null} to indicate that no custom schema was deployed.
     */
    boolean matches(@Nullable SchemaSource source);

    boolean isCqlFirst();
  }

  /** Entry for a CQL-first keyspace. */
  static class LazyCqlFirstGraphqlHolder implements GraphqlHolder {

    private final Supplier<GraphQL> graphqlSupplier;

    LazyCqlFirstGraphqlHolder(Keyspace keyspace) {
      graphqlSupplier = Suppliers.memoize(() -> newGraphql(SchemaFactory.newDmlSchema(keyspace)));
    }

    @Override
    public GraphQL getGraphql() {
      return graphqlSupplier.get();
    }

    @Override
    public boolean matches(@Nullable SchemaSource source) {
      return source == null;
    }

    @Override
    public boolean isCqlFirst() {
      return true;
    }
  }

  /**
   * Entry for a schema-first keyspace, when we've detected a new row in the {@code schema_source}
   * table.
   */
  class LazySchemaFirstGraphqlHolder implements GraphqlHolder {

    private final SchemaSource source;
    private final Supplier<GraphQL> graphqlSupplier;

    LazySchemaFirstGraphqlHolder(SchemaSource source, Keyspace keyspace) {
      this.source = source;
      graphqlSupplier =
          Suppliers.memoize(
              () -> {
                ProcessedSchema processedSchema =
                    new SchemaProcessor(persistence, true).process(source.getContents(), keyspace);
                // Check that the data model still matches
                CassandraMigrator.forPersisted()
                    .compute(processedSchema.getMappingModel(), keyspace);
                return processedSchema.getGraphql();
              });
    }

    @Override
    public GraphQL getGraphql() {
      return graphqlSupplier.get();
    }

    @Override
    public boolean matches(@Nullable SchemaSource otherSource) {
      return otherSource != null && source.getVersion().equals(otherSource.getVersion());
    }

    @Override
    public boolean isCqlFirst() {
      return false;
    }
  }

  /**
   * Entry for a schema-first keyspace, when we already have the GraphQL schema (because we're
   * coming from a deployment operation).
   */
  static class ImmediateSchemaFirstGraphqlHolder implements GraphqlHolder {
    private final SchemaSource source;
    private final GraphQL graphql;

    ImmediateSchemaFirstGraphqlHolder(SchemaSource source, GraphQL graphql) {
      this.source = source;
      this.graphql = graphql;
    }

    @Override
    public GraphQL getGraphql() {
      return graphql;
    }

    @Override
    public boolean matches(@Nullable SchemaSource otherSource) {
      return otherSource != null && this.source.getVersion().equals(otherSource.getVersion());
    }

    @Override
    public boolean isCqlFirst() {
      return false;
    }
  }
}
