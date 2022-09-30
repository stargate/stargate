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

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.grpc.proto.SchemaReads;
import io.stargate.sgv2.restapi.service.models.Sgv2IndexAddRequest;
import io.stargate.sgv2.restapi.service.resources.RestResourceBase;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class Sgv2IndexesResourceImpl extends RestResourceBase implements Sgv2IndexesResourceApi {

  @Override
  public Response getAllIndexesForTable(String keyspaceName, String tableName) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("tableName must be provided", Status.BAD_REQUEST);
    }

    // check that we're authorized for the table
    bridge.authorizeSchemaRead(
        SchemaReads.table(keyspaceName, tableName, Schema.SchemaRead.SourceApi.REST));

    Query query =
        new QueryBuilder()
            .select()
            .from("system_schema", "indexes")
            .where("keyspace_name", Predicate.EQ, Values.of(keyspaceName))
            .where("table_name", Predicate.EQ, Values.of(tableName))
            .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
            .build();
    return fetchRows(query, true);
  }

  @Override
  public Response addIndex(
      final String keyspaceName, final String tableName, final Sgv2IndexAddRequest indexAdd) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("tableName must be provided", Status.BAD_REQUEST);
    }

    String columnName = indexAdd.getColumn();
    if (isStringEmpty(columnName)) {
      throw new WebApplicationException("columnName must be provided", Status.BAD_REQUEST);
    }

    queryWithTable(
        keyspaceName,
        tableName,
        table -> {
          table.getColumnsList().stream()
              .filter(c -> columnName.equals(c.getName()))
              .findAny()
              .orElseThrow(
                  () ->
                      new WebApplicationException(
                          String.format("Column '%s' not found in table.", columnName),
                          Status.NOT_FOUND));
          return new QueryBuilder()
              .create()
              .index(indexAdd.getName())
              .ifNotExists(indexAdd.getIfNotExists())
              .on(keyspaceName, tableName)
              .column(columnName)
              .indexingType(indexAdd.getKind())
              .custom(indexAdd.getType(), indexAdd.getOptions())
              .build();
        });

    Map<String, Object> responsePayload = Collections.singletonMap("success", true);
    return Response.status(Status.CREATED).entity(responsePayload).build();
  }

  @Override
  public Response dropIndex(
      String keyspaceName, String tableName, String indexName, boolean ifExists) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("tableName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(indexName)) {
      throw new WebApplicationException("columnName must be provided", Status.BAD_REQUEST);
    }

    queryWithTable(
        keyspaceName,
        tableName,
        table -> {
          if (!ifExists
              && table.getIndexesList().stream().noneMatch(i -> indexName.equals(i.getName()))) {
            throw new WebApplicationException(
                String.format("Index '%s' not found.", indexName), Status.NOT_FOUND);
          }
          return new QueryBuilder()
              .drop()
              .index(keyspaceName, indexName)
              .ifExists(ifExists)
              .build();
        });
    return Response.status(Status.NO_CONTENT).build();
  }
}
