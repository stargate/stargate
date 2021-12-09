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
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
import io.stargate.sgv2.restsvc.models.Sgv2IndexAddRequest;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import io.stargate.sgv2.restsvc.resources.Sgv2RequestHandler;
import java.util.Collections;
import java.util.Map;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/indexes")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2IndexesResourceImpl extends ResourceBase implements Sgv2IndexesResourceApi {

  @Override
  public Response getAllIndexesForTable(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      HttpServletRequest request) {
    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          if (isStringEmpty(keyspaceName)) {
            return jaxrsBadRequestError("keyspaceName must be provided").build();
          }
          if (isStringEmpty(tableName)) {
            return jaxrsBadRequestError("tableName must be provided").build();
          }

          BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);

          String cql =
              new QueryBuilder()
                  .select()
                  .from("system_schema", "indexes")
                  .where("keyspace_name", Predicate.EQ, keyspaceName)
                  .where("table_name", Predicate.EQ, tableName)
                  .build();
          return fetchRows(blockingStub, -1, null, true, cql, null);
        });
  }

  @Override
  public Response addIndex(
      StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String tableName,
      final Sgv2IndexAddRequest indexAdd,
      HttpServletRequest request) {
    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          if (isStringEmpty(keyspaceName)) {
            return jaxrsBadRequestError("keyspaceName must be provided").build();
          }
          if (isStringEmpty(tableName)) {
            return jaxrsBadRequestError("tableName must be provided").build();
          }

          String columnName = indexAdd.getColumn();
          if (isStringEmpty(columnName)) {
            return jaxrsBadRequestError("columnName must be provided").build();
          }

          Schema.CqlTable table =
              BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
          if (table.getColumnsList().stream().noneMatch(c -> columnName.equals(c.getName()))) {
            return jaxrsServiceError(
                    Response.Status.NOT_FOUND,
                    String.format("Column '%s' not found in table.", columnName))
                .build();
          }

          String cql =
              new QueryBuilder()
                  .create()
                  .index(indexAdd.getName())
                  .ifNotExists(indexAdd.getIfNotExists())
                  .on(keyspaceName, tableName)
                  .column(columnName)
                  .indexingType(indexAdd.getKind())
                  .custom(indexAdd.getType(), indexAdd.getOptions())
                  .build();
          blockingStub.executeQuery(Query.newBuilder().setCql(cql).build());

          Map<String, Object> responsePayload = Collections.singletonMap("success", true);
          return jaxrsResponse(Response.Status.CREATED).entity(responsePayload).build();
        });
  }

  @Override
  public Response dropIndex(
      StargateGrpc.StargateBlockingStub blockingStub,
      String keyspaceName,
      String tableName,
      String indexName,
      boolean ifExists,
      HttpServletRequest request) {
    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          if (isStringEmpty(keyspaceName)) {
            return jaxrsBadRequestError("keyspaceName must be provided").build();
          }
          if (isStringEmpty(tableName)) {
            return jaxrsBadRequestError("tableName must be provided").build();
          }
          if (isStringEmpty(indexName)) {
            return jaxrsBadRequestError("columnName must be provided").build();
          }

          Schema.CqlTable table =
              BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
          if (!ifExists
              && table.getIndexesList().stream().noneMatch(i -> indexName.equals(i.getName()))) {
            return jaxrsServiceError(
                    Response.Status.NOT_FOUND, String.format("Index '%s' not found.", indexName))
                .build();
          }

          String cql =
              new QueryBuilder().drop().index(keyspaceName, indexName).ifExists(ifExists).build();
          blockingStub.executeQuery(Query.newBuilder().setCql(cql).build());

          return jaxrsResponse(Response.Status.NO_CONTENT).build();
        });
  }
}
