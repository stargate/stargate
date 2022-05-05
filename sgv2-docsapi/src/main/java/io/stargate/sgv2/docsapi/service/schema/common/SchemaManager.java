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
import io.stargate.sgv2.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import java.util.Objects;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class SchemaManager {

  @Inject
  @CacheName("keyspace-cache")
  Cache keyspaceCache;

  @Inject GrpcClients grpcClients;

  @Inject Schema.SchemaRead.SourceApi sourceApi;

  @Inject StargateRequestInfo requestInfo;

  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getKeyspace(String keyspace) {
    StargateBridge bridge = grpcClients.bridgeClient(requestInfo);
    return getKeyspaceInternal(bridge, keyspace);
  }

  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getKeyspaceAuthorized(String keyspace) {
    StargateBridge bridge = grpcClients.bridgeClient(requestInfo);

    // call bridge to authorize
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

  // authorizes a keyspace by provided name, no transformations applied
  public Uni<Boolean> authorizeKeyspaceInternal(StargateBridge bridge, String keyspaceName) {
    // first authorize read, then fetch
    Schema.SchemaRead build =
        Schema.SchemaRead.newBuilder()
            .setSourceApi(sourceApi)
            .setKeyspaceName(keyspaceName)
            .setElementType(Schema.SchemaRead.ElementType.KEYSPACE)
            .build();
    Schema.AuthorizeSchemaReadsRequest request =
        Schema.AuthorizeSchemaReadsRequest.newBuilder().addSchemaReads(build).build();

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

  // gets a keyspace by provided name, no transformations applied
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
                          invalidateKeyspace(keyspaceName, tenantId);
                          return cacheKeyspace(keyspaceName, tenantId, updatedKeyspace);
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
                  invalidateKeyspace(keyspaceName, tenantId);
                  return Uni.createFrom().nullItem();
                }
              }

              return Uni.createFrom().failure(t);
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
  protected void invalidateKeyspace(
      @CacheKey String keyspaceName, @CacheKey Optional<String> tenantId) {}
}
