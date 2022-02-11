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
import io.stargate.proto.Schema.SchemaRead;
import java.util.Collections;
import java.util.List;

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
  Response executeQuery(Query request);

  /**
   * Executes a CQL batch.
   *
   * <p>Authorization is automatically handled by the persistence backend.
   */
  Response executeBatch(Batch request);

  /**
   * Gets the metadata describing the given keyspace.
   *
   * @throws UnauthorizedKeyspaceException if the client is not allowed to access this keyspace.
   *     However, this check is shallow: if the caller surfaces keyspace elements (tables, UDTs...)
   *     to the end user, it must authorize them individually with {@link
   *     #authorizeSchemaReads(List)}.
   */
  CqlKeyspaceDescribe getKeyspace(String keyspaceName) throws UnauthorizedKeyspaceException;

  /**
   * Gets the metadata describing all the keyspaces that are visible to this client.
   *
   * <p>This method automatically filters out unauthorized keyspaces. However, this check is
   * shallow: if the caller surfaces keyspace elements (tables, UDTs...) to the end user, it must
   * authorize them individually with {@link #authorizeSchemaReads(List)}.
   */
  List<CqlKeyspaceDescribe> getAllKeyspaces();

  /**
   * Checks whether this client is authorized to describe a set of schema elements.
   *
   * @see SchemaReads
   */
  List<Boolean> authorizeSchemaReads(List<SchemaRead> schemaReads);

  /**
   * Convenience method to call {@link #authorizeSchemaReads(List)} with a single element.
   *
   * @see SchemaReads
   */
  default boolean authorizeSchemaRead(SchemaRead schemaRead) {
    return authorizeSchemaReads(Collections.singletonList(schemaRead)).get(0);
  }
}
