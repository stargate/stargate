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

package io.stargate.sgv2.api.common.schema;

import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
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
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.UnauthorizedKeyspaceException;
import io.stargate.sgv2.api.common.grpc.UnauthorizedTableException;
import io.stargate.sgv2.api.common.grpc.proto.SchemaReads;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
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
   * Get all keyspace from the bridge. Note that this method is not doing any authorization.
   *
   * @return Multi containing Schema.CqlKeyspaceDescribe
   */
  @WithSpan
  public Multi<Schema.CqlKeyspaceDescribe> getKeyspaces() {
    StargateBridge bridge = requestInfo.getStargateBridge();

    // get all names
    return getKeyspaceNames(bridge)

        // then fetch each keyspace
        .onItem()
        .transformToUniAndMerge(keyspace -> getKeyspaceInternal(bridge, keyspace));
  }

  /**
   * Get the table from the bridge. Note that this method is not doing any authorization.
   *
   * @param keyspace Keyspace name
   * @param table Table name
   * @param missingKeyspace Function of the keyspace in case it's not existing. Usually there to
   *     provide a failure.
   * @return Uni containing Schema.CqlTable or <code>null</code> item in case the table does not
   *     exist.
   */
  @WithSpan
  public Uni<Schema.CqlTable> getTable(
      String keyspace,
      String table,
      Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();
    return getTableInternal(bridge, keyspace, table, missingKeyspace);
  }

  /**
   * Get all tables of a keyspace from the bridge.
   *
   * @param keyspace Keyspace name
   * @param missingKeyspace Function of the keyspace in case it's not existing. Usually there to
   *     provide a failure.
   * @return Multi of Schema.CqlTable
   */
  @WithSpan
  public Multi<Schema.CqlTable> getTables(
      String keyspace,
      Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();

    // get keyspace
    return getKeyspaceInternal(bridge, keyspace)

        // if not there, switch to function
        .onItem()
        .ifNull()
        .switchTo(missingKeyspace.apply(keyspace))

        // otherwise get all tables
        .onItem()
        .ifNotNull()
        .transformToMulti(k -> Multi.createFrom().iterable(k.getTablesList()));
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
   * Get all keyspace from the bridge. Prior to getting each keyspace it will execute the schema
   * authorization request (single request for all available keyspace).
   *
   * @return Multi containing Schema.CqlKeyspaceDescribe
   */
  @WithSpan
  public Multi<Schema.CqlKeyspaceDescribe> getKeyspacesAuthorized() {
    StargateBridge bridge = requestInfo.getStargateBridge();

    // get all keyspace names
    return getKeyspaceNames(bridge)

        // collect list
        .collect()
        .asList()

        // then go for the schema read
        .onItem()
        .transformToMulti(
            keyspaceNames -> {
              // if we have no keyspace return immediately
              if (null == keyspaceNames || keyspaceNames.isEmpty()) {
                return Multi.createFrom().empty();
              }

              // create schema reads for all keyspaces
              List<Schema.SchemaRead> reads =
                  keyspaceNames.stream()
                      .map(n -> SchemaReads.keyspace(n, sourceApi))
                      .collect(Collectors.toList());

              Schema.AuthorizeSchemaReadsRequest request =
                  Schema.AuthorizeSchemaReadsRequest.newBuilder().addAllSchemaReads(reads).build();

              // execute request
              return bridge
                  .authorizeSchemaReads(request)

                  // on response filter out
                  .onItem()
                  .ifNotNull()
                  .transformToMulti(
                      response -> {
                        List<String> authorizedKeyspaces = new ArrayList<>(keyspaceNames.size());
                        List<Boolean> authorizedList = response.getAuthorizedList();
                        for (int i = 0; i < authorizedList.size(); i++) {
                          if (authorizedList.get(i)) {
                            authorizedKeyspaces.add(keyspaceNames.get(i));
                          }
                        }
                        // and return all authorized tables
                        return Multi.createFrom().iterable(authorizedKeyspaces);
                      });
            })

        // then fetch each authorized keyspace
        .onItem()
        .transformToUniAndMerge(keyspace -> getKeyspaceInternal(bridge, keyspace));
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
   * @param missingKeyspace Function of the keyspace in case it's not existing. Usually there to
   *     provide a failure.
   * @return Uni containing Schema.CqlTable or <code>null</code> item in case the table does not
   *     exist.
   */
  @WithSpan
  public Uni<Schema.CqlTable> getTableAuthorized(
      String keyspace,
      String table,
      Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
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

  /**
   * Get all authorized tables from the bridge.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Not authorized, with {@link UnauthorizedTableException}
   * </ol>
   *
   * @param keyspace Keyspace name
   * @param missingKeyspace Function of the keyspace in case it's not existing. Usually there to
   *     provide a failure.
   * @return Multi of Schema.CqlTable
   */
  @WithSpan
  public Multi<Schema.CqlTable> getTablesAuthorized(
      String keyspace,
      Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    StargateBridge bridge = requestInfo.getStargateBridge();

    // get keyspace
    return getKeyspaceInternal(bridge, keyspace)

        // if keyspace not found switch to function
        .onItem()
        .ifNull()
        .switchTo(missingKeyspace.apply(keyspace))

        // if it exists go forward
        .onItem()
        .ifNotNull()
        .transformToMulti(
            keyspaceDescribe -> {

              // create schema reads for all tables
              List<Schema.CqlTable> tables = keyspaceDescribe.getTablesList();

              // if empty break immediately
              if (tables.isEmpty()) {
                return Multi.createFrom().empty();
              }

              List<Schema.SchemaRead> reads =
                  tables.stream()
                      .map(t -> SchemaReads.table(keyspace, t.getName(), sourceApi))
                      .collect(Collectors.toList());

              Schema.AuthorizeSchemaReadsRequest request =
                  Schema.AuthorizeSchemaReadsRequest.newBuilder().addAllSchemaReads(reads).build();

              // execute request
              return bridge
                  .authorizeSchemaReads(request)

                  // on response filter out
                  .onItem()
                  .ifNotNull()
                  .transformToMulti(
                      response -> {
                        List<Schema.CqlTable> authorizedTables = new ArrayList<>(tables.size());
                        List<Boolean> authorizedList = response.getAuthorizedList();
                        for (int i = 0; i < authorizedList.size(); i++) {
                          if (authorizedList.get(i)) {
                            authorizedTables.add(tables.get(i));
                          }
                        }

                        // and return all authorized tables
                        return Multi.createFrom().iterable(authorizedTables);
                      });
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
      Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>> missingKeyspace) {
    // get keyspace
    return getKeyspaceInternal(bridge, keyspaceName)

        // if keyspace not found fail always
        .onItem()
        .ifNull()
        .switchTo(missingKeyspace.apply(keyspaceName))
        // otherwise try to find the wanted table
        .onItem()
        .ifNotNull()
        .transformToUni(
            keyspace -> {
              List<Schema.CqlTable> tables = keyspace.getTablesList();
              Optional<Schema.CqlTable> table =
                  tables.stream().filter(t -> Objects.equals(t.getName(), tableName)).findFirst();
              return Uni.createFrom().optional(table);
            });
  }

  // returns all keyspace names
  private Multi<String> getKeyspaceNames(StargateBridge bridge) {
    // repeated multi to achieve paging
    return Multi.createBy()
        .repeating()

        // start with the optimistic state
        .uni(
            () -> new AtomicReference<>(new Paging(true, null)),
            state -> {
              // if we don't have more, short circuit
              Paging paging = state.get();
              if (!paging.hasMore()) {
                return Uni.createFrom().item(Collections.<String>emptyList());
              }

              // otherwise execute the query
              QueryOuterClass.Query.Builder queryBuilder =
                  QueryOuterClass.Query.newBuilder()
                      .setCql("SELECT keyspace_name FROM system_schema.keyspaces");

              QueryOuterClass.QueryParameters.Builder params =
                  QueryOuterClass.QueryParameters.newBuilder().setPageSize(Int32Value.of(100));
              if (null != paging.pageState()) {
                params.setPagingState(paging.pageState());
              }
              queryBuilder.setParameters(params);

              return bridge
                  .executeQuery(queryBuilder.build())
                  .flatMap(
                      response -> {
                        // update the state for the next repetition
                        QueryOuterClass.ResultSet resultSet = response.getResultSet();
                        state.set(
                            new Paging(resultSet.hasPagingState(), resultSet.getPagingState()));

                        // collect all names in a list
                        List<String> keyspaceNames =
                            resultSet.getRowsList().stream()
                                .map(r -> r.getValues(0).getString())
                                .collect(Collectors.toList());

                        return Uni.createFrom().item(keyspaceNames);
                      });
            })

        // do repeat until we get an empty list
        .until(List::isEmpty)

        // then transform list to multi and merge
        .onItem()
        .transformToMultiAndMerge(l -> Multi.createFrom().iterable(l));
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

  // small utility for paging queries
  private record Paging(boolean hasMore, BytesValue pageState) {}
}
