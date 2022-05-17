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

package io.stargate.sgv2.docsapi.service.schema.common;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.extension.annotations.WithSpan;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheName;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CaffeineCache;
import io.quarkus.cache.CompositeCacheKey;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.common.grpc.SchemaReads;
import io.stargate.sgv2.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.common.grpc.UnauthorizedTableException;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class SchemaManager {

  @Inject
  @CacheName("keyspace-cache")
  Cache keyspaceCache;

  @Inject Schema.SchemaRead.SourceApi sourceApi;

  @Inject StargateRequestInfo requestInfo;

  /**
   * Get the keyspace from the bridge. Note that this method is not doing any authorization.
   *
   * @param keyspace Keyspace name
   * @return Uni containing Schema.CqlKeyspaceDescribe or <code>null</code> item in case keyspace
   *     does not exist.
   */
  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getKeyspace(String keyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();
    return getKeyspaceInternal(bridge, keyspace);
  }

  /**
   * Get the table from the bridge. Note that this method is not doing any authorization.
   *
   * @param keyspace Keyspace name
   * @param table Table name
   * @param missingKeyspace Supplier of the keyspace in case it's not existing. Usually there to
   *     provide a failure.
   * @return Uni containing Schema.CqlTable or <code>null</code> item in case the table does not
   *     exist.
   */
  @WithSpan
  public Uni<Schema.CqlTable> getTable(
      String keyspace,
      String table,
      Supplier<Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();
    return getTableInternal(bridge, keyspace, table, missingKeyspace);
  }

  /**
   * Get the keyspace from the bridge. Prior to getting the keyspace it will execute the schema
   * authorization request.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Not authorized, with {@link UnauthorizedKeyspaceException}
   * </ol>
   *
   * @param keyspace Keyspace name
   * @return Uni containing Schema.CqlKeyspaceDescribe or <code>null</code> item in case keyspace
   *     does not exist.
   */
  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getKeyspaceAuthorized(String keyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();

    // first authorize read, then fetch
    return authorizeKeyspaceInternal(bridge, keyspace)

        // on result
        .onItem()
        .transformToUni(
            authorized -> {

              // if authorized, go fetch keyspace
              // otherwise throw correct exception
              if (authorized) {
                return getKeyspaceInternal(bridge, keyspace);
              } else {
                RuntimeException unauthorized = new UnauthorizedKeyspaceException(keyspace);
                return Uni.createFrom().failure(unauthorized);
              }
            });
  }

  /**
   * Get the table from the bridge. Prior to getting the keyspace it will execute the schema
   * authorization request.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Not authorized, with {@link UnauthorizedTableException}
   * </ol>
   *
   * @param keyspace Keyspace name
   * @param table Table name
   * @param missingKeyspace Supplier of the keyspace in case it's not existing. Usually there to
   *     provide a failure.
   * @return Uni containing Schema.CqlTable or <code>null</code> item in case the table does not
   *     exist.
   */
  @WithSpan
  public Uni<Schema.CqlTable> getTableAuthorized(
      String keyspace,
      String table,
      Supplier<Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();

    // first authorize read, then fetch
    return authorizeTableInternal(bridge, keyspace, table)

        // on result
        .onItem()
        .transformToUni(
            authorized -> {

              // if authorized, go fetch keyspace
              // otherwise throw correct exception
              if (authorized) {
                return getTableInternal(bridge, keyspace, table, missingKeyspace);
              } else {
                RuntimeException unauthorized = new UnauthorizedTableException(keyspace, table);
                return Uni.createFrom().failure(unauthorized);
              }
            });
  }

  // authorizes a keyspace by provided name
  public Uni<Boolean> authorizeKeyspaceInternal(StargateBridge bridge, String keyspaceName) {
    Schema.SchemaRead schemaRead = SchemaReads.keyspace(keyspaceName, sourceApi);
    return authorizeInternal(bridge, schemaRead);
  }

  // authorizes a table by provided name and keyspace
  public Uni<Boolean> authorizeTableInternal(
      StargateBridge bridge, String keyspaceName, String tableName) {
    Schema.SchemaRead schemaRead = SchemaReads.table(keyspaceName, tableName, sourceApi);
    return authorizeInternal(bridge, schemaRead);
  }

  // authorizes a single schema read
  public Uni<Boolean> authorizeInternal(StargateBridge bridge, Schema.SchemaRead schemaRead) {
    Schema.AuthorizeSchemaReadsRequest request =
        Schema.AuthorizeSchemaReadsRequest.newBuilder().addSchemaReads(schemaRead).build();

    // call bridge to authorize
    return bridge
        .authorizeSchemaReads(request)

        // on result
        .map(
            response -> {
              // we have only one schema read request
              return response.getAuthorizedList().iterator().next();
            });
  }

  // gets a keyspace by provided name
  private Uni<Schema.CqlKeyspaceDescribe> getKeyspaceInternal(
      StargateBridge bridge, String keyspaceName) {
    Optional<String> tenantId = requestInfo.getTenantId();

    // check if cached, if so we need to revalidate hash
    CompositeCacheKey cacheKey = new CompositeCacheKey(keyspaceName, tenantId);
    boolean cached = keyspaceCache.as(CaffeineCache.class).keySet().contains(cacheKey);

    // get keyspace from cache
    return fetchKeyspace(keyspaceName, tenantId, bridge)

        // check hash still matches
        .flatMap(
            keyspace -> {
              // if it was not cached before, we can simply return
              // no need to check
              if (!cached) {
                return Uni.createFrom().item(keyspace);
              }

              Schema.DescribeKeyspaceQuery request =
                  Schema.DescribeKeyspaceQuery.newBuilder()
                      .setKeyspaceName(keyspaceName)
                      .setHash(keyspace.getHash())
                      .build();

              // call bridge
              return bridge
                  .describeKeyspace(request)
                  .onItem()
                  .transformToUni(
                      updatedKeyspace -> {
                        // if we have updated keyspace cache and return
                        // otherwise return what we had in the cache already
                        if (null != updatedKeyspace && updatedKeyspace.hasCqlKeyspace()) {
                          return invalidateKeyspace(keyspaceName, tenantId)
                              .flatMap(v -> cacheKeyspace(keyspaceName, tenantId, updatedKeyspace));
                        } else {
                          return Uni.createFrom().item(keyspace);
                        }
                      });
            })

        // in case of failure, check if status is not found
        // and invalidate the cache
        .onFailure()
        .recoverWithUni(
            t -> {
              if (t instanceof StatusRuntimeException sre) {
                if (Objects.equals(sre.getStatus().getCode(), Status.Code.NOT_FOUND)) {
                  return invalidateKeyspace(keyspaceName, tenantId)
                      .flatMap(v -> Uni.createFrom().nullItem());
                }
              }

              return Uni.createFrom().failure(t);
            });
  }

  // gets a table by provided name in the given keyspace
  private Uni<Schema.CqlTable> getTableInternal(
      StargateBridge bridge,
      String keyspaceName,
      String tableName,
      Supplier<Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    // get keyspace
    return getKeyspaceInternal(bridge, keyspaceName)

        // if keyspace not found fail always
        .onItem()
        .ifNull()
        .switchTo(missingKeyspace)

        // otherwise try to find the wanted table
        .flatMap(
            keyspace -> {
              List<Schema.CqlTable> tables = keyspace.getTablesList();
              Optional<Schema.CqlTable> table =
                  tables.stream().filter(t -> Objects.equals(t.getName(), tableName)).findFirst();
              return Uni.createFrom().optional(table);
            });
  }

  // fetches the keyspace from the bridge and caches it
  @CacheResult(cacheName = "keyspace-cache")
  protected Uni<Schema.CqlKeyspaceDescribe> fetchKeyspace(
      @CacheKey String keyspaceName, @CacheKey Optional<String> tenantId, StargateBridge bridge) {
    // create request
    Schema.DescribeKeyspaceQuery request =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();

    // memoize
    return bridge.describeKeyspace(request).memoize().indefinitely();
  }

  // simple utility to cache given keyspace
  @CacheResult(cacheName = "keyspace-cache")
  protected Uni<Schema.CqlKeyspaceDescribe> cacheKeyspace(
      @CacheKey String keyspaceName,
      @CacheKey Optional<String> tenantId,
      Schema.CqlKeyspaceDescribe keyspace) {
    return Uni.createFrom().item(keyspace);
  }

  // simple utility to invalidate keyspace
  @CacheInvalidate(cacheName = "keyspace-cache")
  protected Uni<Void> invalidateKeyspace(
      @CacheKey String keyspaceName, @CacheKey Optional<String> tenantId) {
    return Uni.createFrom().nullItem();
  }
}
