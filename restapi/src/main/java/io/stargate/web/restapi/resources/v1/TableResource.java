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
package io.stargate.web.restapi.resources.v1;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.codahale.metrics.annotation.Timed;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.models.ApiError;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.RequestHandler;
import io.stargate.web.restapi.dao.RestDB;
import io.stargate.web.restapi.dao.RestDBFactory;
import io.stargate.web.restapi.models.ClusteringExpression;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.PrimaryKey;
import io.stargate.web.restapi.models.SuccessResponse;
import io.stargate.web.restapi.models.TableAdd;
import io.stargate.web.restapi.models.TableOptions;
import io.stargate.web.restapi.models.TableResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v1/keyspaces/{keyspaceName}/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class TableResource {
  @Inject private RestDBFactory dbFactory;

  @Timed
  @GET
  @ApiOperation(
      value = "Return all tables",
      notes = "Retrieve all tables in a specific keyspace.",
      response = String.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 200,
            message = "OK",
            response = String.class,
            responseContainer = "List"),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  public Response listAllTables(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          List<String> tableNames =
              restDB.getTables(keyspaceName).stream().map(Table::name).collect(Collectors.toList());

          restDB.authorizeSchemaRead(
              Collections.singletonList(keyspaceName),
              tableNames,
              SourceAPI.REST,
              ResourceKind.TABLE);

          return Response.status(Response.Status.OK).entity(tableNames).build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Add a table",
      notes = "Add a table in a specific keyspace.",
      response = SuccessResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = SuccessResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  public Response addTable(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Table object that needs to be added to the keyspace", required = true)
          @NotNull
          final TableAdd tableAdd,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          Keyspace keyspace = restDB.getKeyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new ApiError(
                        "keyspace does not exists", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          String tableName = tableAdd.getName();
          if (tableName == null || tableName.equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new ApiError("table name must be provided"))
                .build();
          }

          PrimaryKey primaryKey = tableAdd.getPrimaryKey();
          if (primaryKey == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new ApiError("primary key must be provided"))
                .build();
          }

          List<Column> columns = new ArrayList<>();
          TableOptions options = tableAdd.getTableOptions();
          for (ColumnDefinition colDef : tableAdd.getColumnDefinitions()) {
            String columnName = colDef.getName();
            if (columnName == null || columnName.equals("")) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(
                      new ApiError(
                          "column name must be provided",
                          Response.Status.BAD_REQUEST.getStatusCode()))
                  .build();
            }

            Kind kind = Converters.getColumnKind(colDef, primaryKey);
            ColumnType type = Type.fromCqlDefinitionOf(keyspace, colDef.getTypeDefinition());
            Order order;
            try {
              order = kind == Kind.Clustering ? Converters.getColumnOrder(colDef, options) : null;
            } catch (Exception e) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(
                      new ApiError(
                          "Unable to create table options " + e.getMessage(),
                          Response.Status.BAD_REQUEST.getStatusCode()))
                  .build();
            }
            columns.add(Column.create(columnName, kind, type, order));
          }

          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.CREATE, SourceAPI.REST, ResourceKind.TABLE);

          int ttl = 0;
          if (options != null && options.getDefaultTimeToLive() != null) {
            ttl = options.getDefaultTimeToLive();
          }

          restDB
              .queryBuilder()
              .create()
              .table(keyspaceName, tableName)
              .ifNotExists(tableAdd.getIfNotExists())
              .column(columns)
              .withDefaultTTL(ttl)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();
          return Response.status(Response.Status.CREATED).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Return a table",
      notes = "Retrieve data for a single table in a specific keyspace.",
      response = TableResponse.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = TableResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("/{tableName}")
  public Response getOneTable(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          restDB.authorizeSchemaRead(
              Collections.singletonList(keyspaceName),
              Collections.singletonList(tableName),
              SourceAPI.REST,
              ResourceKind.TABLE);

          AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

          final List<ColumnDefinition> columnDefinitions =
              tableMetadata.columns().stream()
                  .map(
                      (col) -> {
                        ColumnType type = col.type();
                        return new ColumnDefinition(
                            col.name(),
                            type == null ? null : type.cqlDefinition(),
                            col.kind() == Kind.Static);
                      })
                  .collect(Collectors.toList());

          final List<String> partitionKey =
              tableMetadata.partitionKeyColumns().stream()
                  .map(Column::name)
                  .collect(Collectors.toList());
          final List<String> clusteringKey =
              tableMetadata.clusteringKeyColumns().stream()
                  .map(Column::name)
                  .collect(Collectors.toList());
          final List<ClusteringExpression> clusteringExpression =
              tableMetadata.clusteringKeyColumns().stream()
                  .map(
                      (col) ->
                          new ClusteringExpression(
                              col.name(), Objects.requireNonNull(col.order()).name()))
                  .collect(Collectors.toList());

          final PrimaryKey primaryKey = new PrimaryKey(partitionKey, clusteringKey);
          final int ttl = 0; // no way to get this without querying schema currently
          final TableOptions tableOptions = new TableOptions(ttl, clusteringExpression);

          return Response.ok(
                  new TableResponse(
                      tableMetadata.name(),
                      keyspaceName,
                      columnDefinitions,
                      primaryKey,
                      tableOptions))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(
      value = "Delete a table",
      notes = "Delete a single table in the specified keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("/{tableName}")
  public Response deleteTable(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.DROP, SourceAPI.REST, ResourceKind.TABLE);

          restDB
              .queryBuilder()
              .drop()
              .table(keyspaceName, tableName)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }
}
