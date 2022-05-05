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
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import io.smallrye.mutiny.Uni;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import java.util.Objects;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class SchemaManager {

  Schema.SchemaRead.SourceApi sourceApi;

  @Inject GrpcClients clients;

  @Inject StargateRequestInfo requestInfo;

  // TODO getKeyspaceAuthorized do we really need this?
  // TODO keyspace name mapper
  // TODO bridge in the request info
  // TODO source API as the parameter
  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getKeyspaceAuthorized(String keyspaceName) {
    // first authorize read, then fetch
    StargateBridge bridge = clients.bridgeClient(requestInfo);

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
        .onItem()
        .transformToUni(
            response -> {

              // we have only one schema read request
              Boolean authorized = response.getAuthorizedList().iterator().next();

              // if authorized, go fetch keyspace
              // otherwise throw correct exception
              if (authorized) {
                return getKeyspace(keyspaceName);
              } else {
                // TODO correct exception and mapping
                RuntimeException unauthorized = new RuntimeException("Unauthorized");
                return Uni.createFrom().failure(unauthorized);
              }
            });
  }

  @WithSpan
  public Uni<Schema.CqlKeyspaceDescribe> getKeyspace(String keyspaceName) {
    StargateBridge bridge = clients.bridgeClient(requestInfo);

    // get keyspace from cache
    return fetchKeyspace(keyspaceName, bridge)

        // check hash still matches
        .flatMap(
            keyspace -> {
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
                          invalidateKeyspace(keyspaceName);
                          return cacheKeyspace(keyspaceName, updatedKeyspace);
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
                  invalidateKeyspace(keyspaceName);
                  return Uni.createFrom().nullItem();
                }
              }

              return Uni.createFrom().failure(t);
            });
  }

  // fetches the keyspace from the bridge and caches it
  @CacheResult(cacheName = "keyspace-cache")
  protected Uni<Schema.CqlKeyspaceDescribe> fetchKeyspace(
      @CacheKey String keyspaceName, StargateBridge bridge) {
    // create request
    Schema.DescribeKeyspaceQuery request =
        Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();

    // memoize
    return bridge.describeKeyspace(request).memoize().indefinitely();
  }

  // simple utility to cache given keyspace
  @CacheResult(cacheName = "keyspace-cache")
  protected Uni<Schema.CqlKeyspaceDescribe> cacheKeyspace(
      @CacheKey String keyspaceName, Schema.CqlKeyspaceDescribe keyspace) {
    return Uni.createFrom().item(keyspace);
  }

  // simple utility to invalidate keyspace
  @CacheInvalidate(cacheName = "keyspace-cache")
  protected void invalidateKeyspace(@CacheKey String keyspaceName) {}
}
