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
package io.stargate.sgv2.restsvc.resources.schemas;

import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.Schema.CqlTable;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.common.http.CreateStargateBridgeClient;
import io.stargate.sgv2.restsvc.models.Sgv2IndexAddRequest;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.Collections;
import java.util.Map;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/indexes")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateStargateBridgeClient
public class Sgv2IndexesResourceImpl extends ResourceBase implements Sgv2IndexesResourceApi {

  @Override
  public Response getAllIndexesForTable(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("tableName must be provided", Status.BAD_REQUEST);
    }

    // check that we're authorized for the table
    bridge.getTable(keyspaceName, tableName);

    Query query =
        new QueryBuilder()
            .select()
            .from("system_schema", "indexes")
            .where("keyspace_name", Predicate.EQ, keyspaceName)
            .where("table_name", Predicate.EQ, tableName)
            .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
            .build();
    return fetchRows(bridge, query, true);
  }

  @Override
  public Response addIndex(
      StargateBridgeClient bridge,
      final String keyspaceName,
      final String tableName,
      final Sgv2IndexAddRequest indexAdd,
      HttpServletRequest request) {
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

    bridge.getTable(keyspaceName, tableName)
        .orElseThrow(() -> new WebApplicationException("Table not found", Status.NOT_FOUND))
        .getColumnsList().stream()
        .filter(c -> columnName.equals(c.getName()))
        .findAny()
        .orElseThrow(
            () ->
                new WebApplicationException(
                    String.format("Column '%s' not found in table.", columnName),
                    Status.NOT_FOUND));

    Query query =
        new QueryBuilder()
            .create()
            .index(indexAdd.getName())
            .ifNotExists(indexAdd.getIfNotExists())
            .on(keyspaceName, tableName)
            .column(columnName)
            .indexingType(indexAdd.getKind())
            .custom(indexAdd.getType(), indexAdd.getOptions())
            .build();
    bridge.executeQuery(query);

    Map<String, Object> responsePayload = Collections.singletonMap("success", true);
    return Response.status(Status.CREATED).entity(responsePayload).build();
  }

  @Override
  public Response dropIndex(
      StargateBridgeClient bridge,
      String keyspaceName,
      String tableName,
      String indexName,
      boolean ifExists,
      HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("tableName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(indexName)) {
      throw new WebApplicationException("columnName must be provided", Status.BAD_REQUEST);
    }

    CqlTable table =
        bridge
            .getTable(keyspaceName, tableName)
            .orElseThrow(() -> new WebApplicationException("Table not found", Status.NOT_FOUND));
    if (!ifExists
        && table.getIndexesList().stream().noneMatch(i -> indexName.equals(i.getName()))) {
      throw new WebApplicationException(
          String.format("Index '%s' not found.", indexName), Status.NOT_FOUND);
    }

    Query query =
        new QueryBuilder().drop().index(keyspaceName, indexName).ifExists(ifExists).build();
    bridge.executeQuery(query);

    return Response.status(Status.NO_CONTENT).build();
  }
}
