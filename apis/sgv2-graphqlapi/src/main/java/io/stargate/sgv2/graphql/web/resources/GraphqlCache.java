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
package io.stargate.sgv2.graphql.web.resources;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.futures.Futures;
import io.stargate.sgv2.api.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.api.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.sgv2.graphql.schema.CassandraFetcherExceptionHandler;
import io.stargate.sgv2.graphql.schema.cqlfirst.SchemaFactory;
import io.stargate.sgv2.graphql.schema.graphqlfirst.AdminSchemaBuilder;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.CassandraMigrator;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ProcessedSchema;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.SchemaProcessor;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Manages the {@link GraphQL} instances used by our REST resources.
 *
 * <p>This includes staying up to date with CQL schema changes.
 */
@ApplicationScoped
public class GraphqlCache {

  private final GraphQL ddlGraphql = newGraphql(SchemaFactory.newDdlSchema());
  private final GraphQL schemaFirstAdminGraphql = newGraphql(new AdminSchemaBuilder().build());

  @ConfigProperty(name = "stargate.graphql.enable-default-keyspace")
  boolean enableDefaultKeyspace;

  private volatile CompletionStage<Optional<String>> defaultKeyspaceName;

  private final Cache<String, GraphqlHolder> dmlGraphqlCache =
      Caffeine.newBuilder().maximumSize(1000).expireAfterAccess(5, TimeUnit.MINUTES).build();

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public GraphQL getSchemaFirstAdminGraphql() {
    return schemaFirstAdminGraphql;
  }

  /**
   * @throws UnauthorizedKeyspaceException (wrapped in the future) if the client is not authorized
   *     to read the keyspace's contents.
   */
  public CompletionStage<Optional<GraphQL>> getDmlAsync(
      StargateBridgeClient bridge, String keyspaceName) {

    String decoratedKeyspaceName = bridge.decorateKeyspaceName(keyspaceName);
    GraphqlHolder holder =
        dmlGraphqlCache.get(decoratedKeyspaceName, __ -> new GraphqlHolder(keyspaceName));
    assert holder != null;
    return holder.getGraphql(bridge);
  }

  public Optional<GraphQL> getDml(StargateBridgeClient bridge, String keyspaceName) {
    return Futures.getUninterruptibly(getDmlAsync(bridge, keyspaceName));
  }

  /**
   * Fill the cache entry when it's been computed externally. This is used after a schema-first
   * deployment: we already have the GraphQL since it's been generated in order to validate the
   * deployment, so use it instead of having the cache recompute it.
   */
  public void putDml(
      Schema.CqlKeyspaceDescribe keyspaceDescribe, SchemaSource newSource, GraphQL graphql) {
    Schema.CqlKeyspace keyspace = keyspaceDescribe.getCqlKeyspace();
    GraphqlHolder holder =
        dmlGraphqlCache.get(keyspace.getGlobalName(), __ -> new GraphqlHolder(keyspace.getName()));
    assert holder != null;
    holder.putGraphql(graphql, keyspaceDescribe.getHash().getValue(), newSource);
  }

  public CompletionStage<Optional<String>> getDefaultKeyspaceNameAsync(
      StargateBridgeClient bridge) {
    // Lazy init with double-checked locking:
    CompletionStage<Optional<String>> result = this.defaultKeyspaceName;
    if (result != null) {
      return result;
    }
    synchronized (this) {
      if (defaultKeyspaceName == null) {
        defaultKeyspaceName =
            !enableDefaultKeyspace
                ? CompletableFuture.completedFuture(Optional.empty())
                : new DefaultKeyspaceHelper(bridge).find();
      }
      return defaultKeyspaceName;
    }
  }

  public Optional<String> getDefaultKeyspaceName(StargateBridgeClient bridge) {
    return Futures.getUninterruptibly(getDefaultKeyspaceNameAsync(bridge));
  }

  private static GraphQL newGraphql(GraphQLSchema schema) {
    return GraphQL.newGraphQL(schema)
        .defaultDataFetcherExceptionHandler(CassandraFetcherExceptionHandler.INSTANCE)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(
            new AsyncExecutionStrategy(CassandraFetcherExceptionHandler.INSTANCE))
        .build();
  }

  static class GraphqlHolder {

    private final String keyspaceName;
    private final AtomicReference<GraphqlHolderState> stateRef = new AtomicReference<>(null);

    GraphqlHolder(String keyspaceName) {
      this.keyspaceName = keyspaceName;
    }

    CompletionStage<Optional<GraphQL>> getGraphql(StargateBridgeClient bridge) {

      CompletionStage<Optional<Schema.CqlKeyspaceDescribe>> keyspaceFuture =
          bridge.getKeyspaceAsync(keyspaceName, true);

      return keyspaceFuture.thenCompose(
          maybeKeyspace ->
              maybeKeyspace
                  .map(keyspace -> handleExisting(keyspace, bridge))
                  .orElse(handleMissing()));
    }

    // Handles a possible state change when we know the keyspace doesn't exist.
    private CompletionStage<Optional<GraphQL>> handleMissing() {
      stateRef.set(null);
      return CompletableFuture.completedFuture(Optional.empty());
    }

    // Handles a possible state change when we know the keyspace exists.
    private CompletionStage<Optional<GraphQL>> handleExisting(
        Schema.CqlKeyspaceDescribe keyspace, StargateBridgeClient bridge) {

      // Next step is to check if this is a GraphQL schema-first keyspace
      CompletionStage<Optional<SchemaSource>> sourceFuture =
          new SchemaSourceDao(bridge).getLatestVersionAsync(keyspaceName);

      return sourceFuture.thenCompose(
          maybeSource -> {
            GraphqlHolderState newState =
                new GraphqlHolderState(keyspace.getHash().getValue(), maybeSource);
            GraphqlHolderState oldState = stateRef.get();
            if (newState.equals(oldState)) {
              // The state matches, we already have the latest version
              return oldState.graphqlFuture;
            } else if (stateRef.compareAndSet(oldState, newState)) {
              // We installed our new state, it is our responsibility to recompute
              compute(keyspace, maybeSource, bridge, newState.graphqlFuture);
            } else {
              // Another thread beat us to computing. Assume this is at least our keyspace
              // hash (or even a more recent one).
              newState = stateRef.get();
            }
            return newState.graphqlFuture;
          });
    }

    private void compute(
        Schema.CqlKeyspaceDescribe keyspace,
        Optional<SchemaSource> maybeSource,
        StargateBridgeClient bridge,
        CompletableFuture<Optional<GraphQL>> toComplete) {
      GraphQL graphql =
          maybeSource
              .map(source -> computeSchemaFirst(keyspace, bridge, source))
              .orElseGet(() -> computeCqlFirst(keyspace));
      toComplete.complete(Optional.of(graphql));
    }

    private GraphQL computeSchemaFirst(
        Schema.CqlKeyspaceDescribe keyspace, StargateBridgeClient bridge, SchemaSource source) {
      ProcessedSchema processedSchema =
          new SchemaProcessor(bridge, true).process(source.getContents(), keyspace);
      // Check that the data model still matches
      CassandraMigrator.forPersisted().compute(processedSchema.getMappingModel(), keyspace);
      return processedSchema.getGraphql();
    }

    private GraphQL computeCqlFirst(Schema.CqlKeyspaceDescribe keyspace) {
      return newGraphql(SchemaFactory.newDmlSchema(keyspace));
    }

    void putGraphql(GraphQL graphql, int hash, SchemaSource newSource) {
      GraphqlHolderState newState = new GraphqlHolderState(hash, Optional.of(newSource));
      newState.graphqlFuture.complete(Optional.of(graphql));
      stateRef.set(newState);
    }
  }

  static class GraphqlHolderState {

    // The hash of the CqlKeyspaceDescribe that this GraphQL is based on.
    final int hash;
    // If this is a schema-first GraphQL, the source that it is based on.
    final Optional<SchemaSource> source;
    // The result of the computation of this GraphQL (possibly still in progress).
    final CompletableFuture<Optional<GraphQL>> graphqlFuture;

    GraphqlHolderState(int hash, Optional<SchemaSource> source) {
      this.hash = hash;
      this.source = source;
      this.graphqlFuture = new CompletableFuture<>();
    }

    @Override
    public boolean equals(Object other) {
      // Note: graphqlFuture deliberately omitted
      if (other == this) {
        return true;
      } else if (other instanceof GraphqlHolderState) {
        GraphqlHolderState that = (GraphqlHolderState) other;
        return this.hash == that.hash && this.source.equals(that.source);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(hash, source);
    }
  }
}
