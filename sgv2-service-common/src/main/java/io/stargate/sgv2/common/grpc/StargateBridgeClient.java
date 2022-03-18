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

import io.stargate.proto.QueryOuterClass.Batch;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.proto.Schema.SchemaRead;
import io.stargate.sgv2.common.futures.Futures;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

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
   * @throws UnauthorizedKeyspaceException (wrapped in the returned future) if the client is not
   *     allowed to access this keyspace. However, this check is shallow: if the caller surfaces
   *     keyspace elements (tables, UDTs...) to the end user, it must authorize them individually
   *     with {@link #authorizeSchemaReads(List)}.
   */
  CompletionStage<Optional<CqlKeyspaceDescribe>> getKeyspaceAsync(String keyspaceName);

  /** @see #getKeyspaceAsync(String) */
  default Optional<CqlKeyspaceDescribe> getKeyspace(String keyspaceName)
      throws UnauthorizedKeyspaceException {
    return Futures.getUninterruptibly(getKeyspaceAsync(keyspaceName));
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
   * Converts an "external" keyspace name known to this client to the "internal" one actually used
   * by the persistence backend.
   *
   * <p>Most of the time, this interface deals with this automatically: for example, the CQL queries
   * passed to {@link #executeQuery(Query)} expect the external name; so does {@link
   * #getKeyspace(String)}. Decorating manually is only needed if the caller interacts directly with
   * {@link StargateBridgeSchema} (which uses internal names).
   *
   * <p>Not all persistence backends use such a mapping. If not applicable, this method returns its
   * argument unchanged.
   */
  String decorateKeyspaceName(String keyspaceName);

  /**
   * Gets the metadata describing the given table.
   *
   * @throws UnauthorizedTableException (wrapped in the returned future) if the client is not
   *     allowed to access this table.
   */
  CompletionStage<Optional<CqlTable>> getTableAsync(String keyspaceName, String tableName);

  /** @see #getTableAsync(String, String) */
  default Optional<CqlTable> getTable(String keyspaceName, String tableName)
      throws UnauthorizedTableException {
    return Futures.getUninterruptibly(getTableAsync(keyspaceName, tableName));
  }

  /**
   * Gets the metadata describing all the tables that are visible to this client in the given
   * keyspace.
   *
   * <p>This method automatically filters out unauthorized tables.
   */
  CompletionStage<List<CqlTable>> getTablesAsync(String keyspaceName);

  /** @see #getTableAsync(String, String) */
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
}
