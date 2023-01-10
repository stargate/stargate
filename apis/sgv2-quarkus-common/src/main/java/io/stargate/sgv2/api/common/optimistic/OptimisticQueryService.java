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

package io.stargate.sgv2.api.common.optimistic;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class OptimisticQueryService {

  @Inject private SchemaManager schemaManager;

  @Inject private StargateRequestInfo requestInfo;

  /**
   * Executes the optimistic query, by fetching the keyspace with authorization from the @{@link
   * SchemaManager}.
   *
   * @param keyspace Keyspace name.
   * @param table Table name.
   * @param missingKeyspace Function in case the keyspace in case it's not existing. Usually there
   *     to provide a failure.
   * @param queryFunction Function that creates the query from the {@link Schema.CqlTable}. Note
   *     that the table can be <code>null</code>.
   * @return Response when optimistic query is executed correctly.
   */
  public Uni<QueryOuterClass.Response> executeAuthorizedOptimistic(
      String keyspace,
      String table,
      Function<String, Uni<? extends QueryOuterClass.Response>> missingKeyspace,
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction) {

    // get table from the schema manager authorized without the hash validation
    Uni<Schema.CqlKeyspaceDescribe> keyspaceUni =
        schemaManager.getKeyspaceAuthorized(keyspace, false);
    return executeOptimistic(
        keyspaceUni, table, () -> missingKeyspace.apply(keyspace), queryFunction);
  }

  /**
   * Executes the optimistic query, by fetching the keyspace without authorization from the @{@link
   * SchemaManager}.
   *
   * @param keyspace Keyspace name.
   * @param table Table name.
   * @param missingKeyspace Function in case the keyspace in case it's not existing. Usually there
   *     to * provide a failure.
   * @param queryFunction Function that creates the query from the {@link Schema.CqlTable}. Note
   *     that the table can be <code>null</code>.
   * @return Response when optimistic query is executed correctly.
   */
  public Uni<QueryOuterClass.Response> executeOptimistic(
      String keyspace,
      String table,
      Function<String, Uni<? extends QueryOuterClass.Response>> missingKeyspace,
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction) {

    // get table from the schema manager without the hash validation
    Uni<Schema.CqlKeyspaceDescribe> keyspaceUni = schemaManager.getKeyspace(keyspace, false);
    return executeOptimistic(
        keyspaceUni, table, () -> missingKeyspace.apply(keyspace), queryFunction);
  }

  /**
   * Executes the optimistic query against the keyspace provided in the given uni.
   *
   * @param keyspaceUni Uni providing the keyspace.
   * @param table Table name.
   * @param missingKeyspace Supplier in case the keyspace in case it's not existing. Usually there
   *     to provide a failure.
   * @param queryFunction Function that creates the query from the {@link Schema.CqlTable}. Note
   *     that the table can be <code>null</code>.
   * @return Response when optimistic query is executed correctly.
   */
  public Uni<QueryOuterClass.Response> executeOptimistic(
      Uni<Schema.CqlKeyspaceDescribe> keyspaceUni,
      String table,
      Supplier<Uni<? extends QueryOuterClass.Response>> missingKeyspace,
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction) {

    // get from cql keyspace
    return keyspaceUni

        // if keyspace is found, execute optimistic query with that keyspace
        .onItem()
        .ifNotNull()
        .transformToUni(
            cqlKeyspace ->
                executeWithKeyspace(cqlKeyspace, table, queryFunction)

                    // once we have a result pipe to our handler
                    .onItem()
                    .transformToUni(handler(table, missingKeyspace, queryFunction)))

        // if keyspace not found at first place, use mapper
        .onItem()
        .ifNull()
        .switchTo(missingKeyspace.get());
  }

  // handles the QueryWithSchemaResponse
  private Function<Schema.QueryWithSchemaResponse, Uni<? extends QueryOuterClass.Response>> handler(
      String table,
      Supplier<Uni<? extends QueryOuterClass.Response>> missingKeyspace,
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction) {
    return response -> {

      // * if no keyspace, calls the missing key space function
      // * if there is new keyspace, execute again with that keyspace, use same handler
      // * if everything is fine, map to result
      if (response.hasNoKeyspace()) {
        return missingKeyspace.get();
      } else if (response.hasNewKeyspace()) {
        // note that this is endlessly trying to execute the query in case there is always a changes
        // keyspace
        return executeWithKeyspace(response.getNewKeyspace(), table, queryFunction)
            .onItem()
            .transformToUni(handler(table, missingKeyspace, queryFunction));
      } else {
        return Uni.createFrom().item(response.getResponse());
      }
    };
  }

  // executes the optimistic query against the keyspace
  private Uni<Schema.QueryWithSchemaResponse> executeWithKeyspace(
      Schema.CqlKeyspaceDescribe cqlKeyspace,
      String table,
      Function<Schema.CqlTable, Uni<QueryOuterClass.Query>> queryFunction) {

    // try to find the wanted table
    List<Schema.CqlTable> cqlTables = cqlKeyspace.getTablesList();
    Optional<Schema.CqlTable> cqlTable =
        cqlTables.stream().filter(t -> Objects.equals(t.getName(), table)).findFirst();

    // start sequence from table
    return Uni.createFrom()
        .optional(cqlTable)

        // query function should handle not found table,
        // so just hit
        .flatMap(queryFunction::apply)
        .flatMap(
            query -> {
              // construct request
              Schema.QueryWithSchema request =
                  Schema.QueryWithSchema.newBuilder()
                      .setQuery(query)
                      .setKeyspaceName(cqlKeyspace.getCqlKeyspace().getName())
                      .setKeyspaceHash(cqlKeyspace.getHash().getValue())
                      .build();

              // fire call
              return requestInfo.getStargateBridge().executeQueryWithSchema(request);
            });
  }
}
