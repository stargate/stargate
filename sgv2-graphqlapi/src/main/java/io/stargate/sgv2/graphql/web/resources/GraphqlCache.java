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
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.graphql.schema.CassandraFetcherExceptionHandler;
import io.stargate.sgv2.graphql.schema.cqlfirst.SchemaFactory;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the {@link GraphQL} instances used by our REST resources.
 *
 * <p>This includes staying up to date with CQL schema changes.
 */
public class GraphqlCache {

  private final boolean disableDefaultKeyspace;
  private final GraphQL ddlGraphql;
  private volatile CompletionStage<Optional<String>> defaultKeyspaceName;
  private final Cache<String, GraphqlHolder> dmlGraphqlCache =
      Caffeine.newBuilder().maximumSize(1000).expireAfterAccess(5, TimeUnit.MINUTES).build();

  public GraphqlCache(boolean disableDefaultKeyspace) {
    this.disableDefaultKeyspace = disableDefaultKeyspace;
    this.ddlGraphql = newGraphql(SchemaFactory.newDdlSchema());
  }

  public GraphQL getDdl() {
    return ddlGraphql;
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
            disableDefaultKeyspace
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
    private final AtomicReference<GraphqlHolderState> stateRef =
        new AtomicReference<>(GraphqlHolderState.EMPTY);

    GraphqlHolder(String keyspaceName) {
      this.keyspaceName = keyspaceName;
    }

    CompletionStage<Optional<GraphQL>> getGraphql(StargateBridgeClient bridge) {
      return bridge
          .getKeyspaceAsync(keyspaceName, true)
          .thenCompose(
              maybeKeyspace -> {
                Optional<Integer> hash =
                    maybeKeyspace.map(describe -> describe.getHash().getValue());
                while (true) {
                  GraphqlHolderState state = stateRef.get();
                  // If the hash matches, we already have the latest version
                  if (state.hash.equals(hash)) {
                    return state.graphqlFuture;
                  }
                  // Otherwise, record the new hash and recompute (accounting for concurrent calls)
                  GraphqlHolderState newState =
                      hash.map(GraphqlHolderState::new).orElse(GraphqlHolderState.EMPTY);
                  if (stateRef.compareAndSet(state, newState)) {
                    maybeKeyspace.ifPresent(
                        keyspace -> {
                          GraphQL graphql = newGraphql(SchemaFactory.newDmlSchema(keyspace));
                          newState.graphqlFuture.complete(Optional.of(graphql));
                        });
                    return newState.graphqlFuture;
                  }
                }
              });
    }
  }

  static class GraphqlHolderState {

    static GraphqlHolderState EMPTY =
        new GraphqlHolderState(
            Optional.empty(), CompletableFuture.completedFuture(Optional.empty()));

    final Optional<Integer> hash;
    final CompletableFuture<Optional<GraphQL>> graphqlFuture;

    GraphqlHolderState(Integer hash) {
      this.hash = Optional.of(hash);
      this.graphqlFuture = new CompletableFuture<>();
    }

    private GraphqlHolderState(
        Optional<Integer> hash, CompletableFuture<Optional<GraphQL>> graphqlFuture) {
      this.hash = hash;
      this.graphqlFuture = graphqlFuture;
    }
  }
}
