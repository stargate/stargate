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
package io.stargate.sgv2.restapi.service.resources.schemas;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.config.RequestParams;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restapi.config.RestApiUtils;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import io.stargate.sgv2.restapi.service.resources.RestResourceBase;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.jboss.resteasy.reactive.RestResponse;

public class Sgv2IndexesResourceImpl extends RestResourceBase implements Sgv2IndexesResourceApi {

  @Override
  public Uni<RestResponse<Object>> getAllIndexes(
      String keyspaceName, String tableName, final Boolean compactMap) {
    final RequestParams requestParams = RestApiUtils.getRequestParams(restApiConfig, compactMap);
    Query query =
        new QueryBuilder()
            .select()
            .from("system_schema", "indexes")
            .where("keyspace_name", Predicate.EQ, Values.of(keyspaceName))
            .where("table_name", Predicate.EQ, Values.of(tableName))
            .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
            .build();

    // This call will authorize lookup on "table" so no separate access checks
    // should be needed
    return getTableAsyncCheckExistence(keyspaceName, tableName, true, Response.Status.BAD_REQUEST)
        .flatMap(table -> executeQueryAsync(query))
        .map(response -> convertRowsToResponse(response, true, requestParams));
  }

  @Override
  public Uni<RestResponse<Map<String, Object>>> addIndex(
      final String keyspaceName, final String tableName, final Sgv2IndexAddRequest indexAdd) {
    final String columnName = indexAdd.getColumn();
    return getTableAsyncCheckExistence(keyspaceName, tableName, true, Response.Status.BAD_REQUEST)
        .flatMap(
            table -> {
              if (!hasColumn(table, columnName)) {
                throw new WebApplicationException(
                    String.format("Column '%s' not found in table '%s'", columnName, tableName),
                    Status.NOT_FOUND);
              }
              return executeQueryAsync(
                  new QueryBuilder()
                      .create()
                      .index(indexAdd.getName())
                      .ifNotExists(indexAdd.getIfNotExists())
                      .on(keyspaceName, tableName)
                      .column(columnName)
                      .indexingType(indexAdd.getKind())
                      .custom(indexAdd.getType(), indexAdd.getOptions())
                      .build());
            })
        .map(any -> RestResponse.status(Status.CREATED, Collections.singletonMap("success", true)));
  }

  @Override
  public Uni<RestResponse<Void>> deleteIndex(
      String keyspaceName, String tableName, String indexName, boolean ifExists) {
    // Use 'getTableAsyncCheckExistence' to check existence of Keyspace and Table as well as access;
    // existence and access to Index itself verified by persistence backend
    return getTableAsyncCheckExistence(keyspaceName, tableName, true, Response.Status.BAD_REQUEST)
        .flatMap(
            table ->
                executeQueryAsync(
                    new QueryBuilder()
                        .drop()
                        .index(keyspaceName, indexName)
                        .ifExists(ifExists)
                        .build()))
        .map(any -> RestResponse.noContent());
  }

  private boolean hasColumn(Schema.CqlTable table, String columnName) {
    List<QueryOuterClass.ColumnSpec> s = table.getColumnsList();
    return hasColumn(table.getColumnsList(), columnName)
        || hasColumn(table.getPartitionKeyColumnsList(), columnName)
        || hasColumn(table.getClusteringKeyColumnsList(), columnName);
  }

  private boolean hasColumn(List<QueryOuterClass.ColumnSpec> columns, String nameToFind) {
    return columns.stream().anyMatch(c -> nameToFind.equals(c.getName()));
  }
}
