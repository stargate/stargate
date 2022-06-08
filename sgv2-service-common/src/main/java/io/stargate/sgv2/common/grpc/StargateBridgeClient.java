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
package io.stargate.sgv2.common.grpc;

import io.stargate.bridge.proto.QueryOuterClass.Batch;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.Schema.CqlTable;
import io.stargate.bridge.proto.Schema.SchemaRead;
import io.stargate.bridge.proto.Schema.SupportedFeaturesResponse;
import io.stargate.sgv2.common.futures.Futures;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The client that allows Stargate services to communicate with the "bridge" to the persistence
 * backend.
 */
public interface StargateBridgeClient {

  /**
   * Executes a CQL query.
   *
   * <p>Authorization is automatically handled by the persistence backend.
   */
  CompletionStage<Response> executeQueryAsync(Query query);

  /** @see #executeQueryAsync(Query) */
  default Response executeQuery(Query query) {
    return Futures.getUninterruptibly(executeQueryAsync(query));
  }

  // TODO create async variant?
  Response executeQuery(
      String keyspaceName, Function<Optional<CqlKeyspaceDescribe>, Query> queryProducer);

  default Response executeQuery(
      String keyspaceName, String tableName, Function<Optional<CqlTable>, Query> queryProducer) {
    return executeQuery(
        keyspaceName,
        maybeKeyspace -> {
          Optional<CqlTable> maybeTable =
              maybeKeyspace.flatMap(
                  keyspace ->
                      keyspace.getTablesList().stream()
                          .filter(t -> t.getName().equals(tableName))
                          .findFirst());
          return queryProducer.apply(maybeTable);
        });
  }

  /**
   * Executes a CQL batch.
   *
   * <p>Authorization is automatically handled by the persistence backend.
   */
  CompletionStage<Response> executeBatchAsync(Batch batch);

  /** @see #executeBatchAsync(Batch) */
  default Response executeBatch(Batch batch) {
    return Futures.getUninterruptibly(executeBatchAsync(batch));
  }

  /**
   * Gets the metadata describing the given keyspace.
   *
   * @param checkIfAuthorized whether to check if the current user is allowed to describe this
   *     keyspace (this requires an additional background call to the bridge). This is an
   *     optimization: if you only use the schema metadata to build a DML query that will be
   *     executed immediately after, you can skip authorization since {@link
   *     #executeQueryAsync(Query)} does it automatically. However, if there is no other query and
   *     you build a client response directly from the metadata, then authorization should be done
   *     by this method.
   * @throws UnauthorizedKeyspaceException (wrapped in the returned future) if {@code
   *     checkIfAuthorized} is set, and the client is not allowed to access this keyspace. However,
   *     this check is shallow: if the caller surfaces keyspace elements (tables, UDTs...) to the
   *     end user, it must authorize them individually with {@link #authorizeSchemaReads(List)}.
   */
  CompletionStage<Optional<CqlKeyspaceDescribe>> getKeyspaceAsync(
      String keyspaceName, boolean checkIfAuthorized);

  /** @see #getKeyspaceAsync(String, boolean) */
  default Optional<CqlKeyspaceDescribe> getKeyspace(String keyspaceName, boolean checkIfAuthorized)
      throws UnauthorizedKeyspaceException {
    return Futures.getUninterruptibly(getKeyspaceAsync(keyspaceName, checkIfAuthorized));
  }

  /**
   * Gets the metadata describing all the keyspaces that are visible to this client.
   *
   * <p>This method automatically filters out unauthorized keyspaces. However, this check is
   * shallow: if the caller surfaces keyspace elements (tables, UDTs...) to the end user, it must
   * authorize them individually with {@link #authorizeSchemaReads(List)}.
   */
  CompletionStage<List<CqlKeyspaceDescribe>> getAllKeyspacesAsync();

  /** @see #getAllKeyspacesAsync() */
  default List<CqlKeyspaceDescribe> getAllKeyspaces() {
    return Futures.getUninterruptibly(getAllKeyspacesAsync());
  }

  /**
   * Converts a keyspace name used by this client to the "global" one actually used by the
   * persistence backend. In other words, this provides a way to compute {@link
   * Schema.CqlKeyspace#getGlobalName()} from {@link Schema.CqlKeyspace#getName()}.
   */
  String decorateKeyspaceName(String keyspaceName);

  /**
   * Gets the metadata describing the given table.
   *
   * @param checkIfAuthorized see explanations in {@link #getKeyspaceAsync(String, boolean)}.
   * @throws UnauthorizedTableException (wrapped in the returned future) if {@code
   *     checkIfAuthorized} is set, and the client is not allowed to access this table.
   */
  CompletionStage<Optional<CqlTable>> getTableAsync(
      String keyspaceName, String tableName, boolean checkIfAuthorized);

  /** @see #getTableAsync(String, String, boolean) */
  default Optional<CqlTable> getTable(
      String keyspaceName, String tableName, boolean checkIfAuthorized)
      throws UnauthorizedTableException {
    return Futures.getUninterruptibly(getTableAsync(keyspaceName, tableName, checkIfAuthorized));
  }

  /**
   * Gets the metadata describing all the tables that are visible to this client in the given
   * keyspace.
   *
   * <p>This method automatically filters out unauthorized tables.
   */
  CompletionStage<List<CqlTable>> getTablesAsync(String keyspaceName);

  /** @see #getTableAsync(String, String, boolean) */
  default List<CqlTable> getTables(String keyspaceName) {
    return Futures.getUninterruptibly(getTablesAsync(keyspaceName));
  }

  /**
   * Checks whether this client is authorized to describe a set of schema elements.
   *
   * @see SchemaReads
   */
  CompletionStage<List<Boolean>> authorizeSchemaReadsAsync(List<SchemaRead> schemaReads);

  /**
   * @see #authorizeSchemaReadsAsync(List)
   * @see SchemaReads
   */
  default List<Boolean> authorizeSchemaReads(List<SchemaRead> schemaReads) {
    return Futures.getUninterruptibly(authorizeSchemaReadsAsync(schemaReads));
  }

  /**
   * Convenience method to call {@link #authorizeSchemaReadsAsync(List)} with a single element.
   *
   * @see SchemaReads
   */
  default CompletionStage<Boolean> authorizeSchemaReadAsync(SchemaRead schemaRead) {
    return authorizeSchemaReadsAsync(Collections.singletonList(schemaRead))
        .thenApply(l -> l.get(0));
  }

  /**
   * Convenience method to call {@link #authorizeSchemaReads(List)} with a single element.
   *
   * @see SchemaReads
   */
  default boolean authorizeSchemaRead(SchemaRead schemaRead) {
    return authorizeSchemaReads(Collections.singletonList(schemaRead)).get(0);
  }

  /** Checks which features are supported by the persistence backend. */
  CompletionStage<SupportedFeaturesResponse> getSupportedFeaturesAsync();

  /** @see #getSupportedFeaturesAsync() */
  default SupportedFeaturesResponse getSupportedFeatures() {
    return Futures.getUninterruptibly(getSupportedFeaturesAsync());
  }
}
