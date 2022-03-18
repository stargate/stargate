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

import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.GraphQLSchema;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.futures.Futures;
import io.stargate.sgv2.common.grpc.KeyspaceInvalidationListener;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.grpc.StargateBridgeSchema;
import io.stargate.sgv2.graphql.schema.CassandraFetcherExceptionHandler;
import io.stargate.sgv2.graphql.schema.cqlfirst.SchemaFactory;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages the {@link GraphQL} instances used by our REST resources.
 *
 * <p>This includes staying up to date with CQL schema changes.
 */
public class GraphqlCache implements KeyspaceInvalidationListener {

  private final StargateBridgeSchema schema;
  private final GraphQL ddlGraphql;
  private final Optional<String> defaultKeyspaceName;
  private final ConcurrentMap<String, CompletionStage<Optional<GraphQL>>> dmlGraphqls =
      new ConcurrentHashMap<>();

  public GraphqlCache(
      StargateBridgeClient adminClient,
      StargateBridgeSchema schema,
      boolean disableDefaultKeyspace) {
    this.schema = schema;
    this.ddlGraphql = newGraphql(SchemaFactory.newDdlSchema());
    this.defaultKeyspaceName =
        disableDefaultKeyspace ? Optional.empty() : new DefaultKeyspaceHelper(adminClient).find();

    schema.register(this);
  }

  public GraphQL getDdl() {
    return ddlGraphql;
  }

  public CompletionStage<Optional<GraphQL>> getDmlAsync(
      String decoratedKeyspaceName, String keyspaceName) {

    CompletionStage<Optional<GraphQL>> existing = dmlGraphqls.get(decoratedKeyspaceName);
    if (existing != null) {
      return existing;
    }
    CompletableFuture<Optional<GraphQL>> mine = new CompletableFuture<>();
    existing = dmlGraphqls.putIfAbsent(decoratedKeyspaceName, mine);
    if (existing != null) {
      return existing;
    }
    loadAsync(decoratedKeyspaceName, keyspaceName, mine);
    return mine;
  }

  public Optional<GraphQL> getDml(String decoratedKeyspaceName, String keyspaceName) {
    return Futures.getUninterruptibly(getDmlAsync(decoratedKeyspaceName, keyspaceName));
  }

  public Optional<String> getDefaultKeyspaceName() {
    return defaultKeyspaceName;
  }

  @Override
  public void onKeyspaceInvalidated(String keyspaceName) {
    dmlGraphqls.remove(keyspaceName);
  }

  private void loadAsync(
      String decoratedKeyspaceName,
      String keyspaceName,
      CompletableFuture<Optional<GraphQL>> toComplete) {
    schema
        .getKeyspaceAsync(decoratedKeyspaceName)
        .thenAccept(cqlSchema -> toComplete.complete(buildDml(cqlSchema, keyspaceName)))
        .exceptionally(
            throwable -> {
              // Surface to the caller, but don't leave a failed entry in the cache
              toComplete.completeExceptionally(throwable);
              dmlGraphqls.remove(decoratedKeyspaceName);
              return null;
            });
  }

  private Optional<GraphQL> buildDml(CqlKeyspaceDescribe cqlSchema, String keyspaceName) {
    return Optional.ofNullable(cqlSchema)
        .map(s -> newGraphql(SchemaFactory.newDmlSchema(s, keyspaceName)));
  }

  private static GraphQL newGraphql(GraphQLSchema schema) {
    return GraphQL.newGraphQL(schema)
        .defaultDataFetcherExceptionHandler(CassandraFetcherExceptionHandler.INSTANCE)
        // Use parallel execution strategy for mutations (serial is default)
        .mutationExecutionStrategy(
            new AsyncExecutionStrategy(CassandraFetcherExceptionHandler.INSTANCE))
        .build();
  }
}
