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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.schema;

import io.opentelemetry.extension.annotations.WithSpan;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.schema.query.NamespaceQueryProvider;
import java.util.function.Function;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Namespace manager provides basic operations on namespace (CQL keyspaces). Note that this manager
 * always uses authorized operations on the {@link SchemaManager}
 */
@ApplicationScoped
public class NamespaceManager {

  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE_FUNCTION =
          keyspace -> {
            String message = "Namespace %s does not exist.".formatted(keyspace);
            Exception exception =
                new ErrorCodeRuntimeException(ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST, message);
            return Uni.createFrom().failure(exception);
          };

  @Inject SchemaManager schemaManager;

  @Inject StargateRequestInfo requestInfo;

  @Inject NamespaceQueryProvider queryProvider;

  /**
   * Creates a namespace with specified replication using the ifNotExists strategy.
   *
   * @param namespace Namespace name.
   * @param replication Replication
   * @return Always returns Uni containing <code>null</code> item or failure
   */
  @WithSpan
  public Uni<Void> createNamespace(String namespace, Replication replication) {
    // create query
    QueryOuterClass.Query query = queryProvider.createNamespaceQuery(namespace, replication);
    StargateBridge bridge = requestInfo.getStargateBridge();

    // execute request
    return bridge
        .executeQuery(query)

        // on result simply return
        .map(any -> null);
  }

  /**
   * Deletes a namespace.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   * </ol>
   *
   * @param namespace Namespace name.
   * @return Always returns Uni containing <code>null</code> item or failure
   */
  @WithSpan
  public Uni<Void> dropNamespace(String namespace) {
    // get keyspace first
    return getNamespaceInternal(namespace)

        // if exists, try to delete
        .onItem()
        .ifNotNull()
        .transformToUni(
            keyspace -> {

              // create delete query
              QueryOuterClass.Query query = queryProvider.deleteNamespaceQuery(namespace);
              StargateBridge bridge = requestInfo.getStargateBridge();

              // execute request
              return bridge
                  .executeQuery(query)

                  // on successful call simply return
                  .map(any -> null);
            });
  }

  /**
   * Gets a single namespace.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#DATASTORE_KEYSPACE_DOES_NOT_EXIST}
   * </ol>
   *
   * @param namespace Namespace name.
   * @return Uni with {@link Schema.CqlKeyspaceDescribe}
   */
  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getNamespace(String namespace) {
    return getNamespaceInternal(namespace);
  }

  /**
   * Gets all available namespaces.
   *
   * @return Multi of {@link Schema.CqlKeyspaceDescribe}
   */
  @WithSpan
  public Multi<Schema.CqlKeyspaceDescribe> getNamespaces() {
    return schemaManager.getKeyspacesAuthorized();
  }

  private Uni<Schema.CqlKeyspaceDescribe> getNamespaceInternal(String namespace) {

    // get the keyspace
    return schemaManager
        .getKeyspaceAuthorized(namespace)

        // if not there error
        .onItem()
        .ifNull()
        .switchTo(MISSING_KEYSPACE_FUNCTION.apply(namespace));
  }
}
