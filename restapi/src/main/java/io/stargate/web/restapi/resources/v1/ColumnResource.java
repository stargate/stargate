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
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Keyspace;
import io.stargate.web.models.ApiError;
import io.stargate.web.resources.RequestHandler;
import io.stargate.web.restapi.dao.RestDB;
import io.stargate.web.restapi.dao.RestDBFactory;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.models.ColumnUpdate;
import io.stargate.web.restapi.models.SuccessResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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
@Path("/v1/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ColumnResource {

  private RestDBFactory dbFactory;

  @Inject
  public ColumnResource(RestDBFactory db) {
    this.dbFactory = db;
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve all columns",
      notes = "Return all columns for a specified table.",
      response = ColumnDefinition.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 200,
            message = "OK",
            response = ColumnDefinition.class,
            responseContainer = "List"),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  public Response listAllColumns(
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

          final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

          List<ColumnDefinition> collect =
              tableMetadata.columns().stream()
                  .map(
                      (col) -> {
                        //noinspection ConstantConditions
                        String type = col.type() != null ? col.type().cqlDefinition() : null;
                        return new ColumnDefinition(
                            col.name(), type, col.kind() == Column.Kind.Static);
                      })
                  .collect(Collectors.toList());

          return Response.status(Response.Status.OK).entity(collect).build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Add a column",
      notes = "Add a single column to a table.",
      response = SuccessResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = SuccessResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 409, message = "Conflict", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  public Response addColumn(
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
      @ApiParam(value = "", required = true) @NotNull final ColumnDefinition columnDefinition,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          Keyspace keyspace = restDB.getKeyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new ApiError(
                        String.format("keyspace '%s' not found", keyspaceName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          String name = columnDefinition.getName();
          Kind kind = Kind.Regular;
          if (columnDefinition.getIsStatic()) {
            kind = Kind.Static;
          }

          Column column =
              ImmutableColumn.builder()
                  .name(name)
                  .kind(kind)
                  .type(
                      Column.Type.fromCqlDefinitionOf(
                          keyspace, columnDefinition.getTypeDefinition()))
                  .build();

          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.ALTER, SourceAPI.REST, ResourceKind.TABLE);

          restDB
              .queryBuilder()
              .alter()
              .table(keyspaceName, tableName)
              .addColumn(column)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.CREATED).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve a column",
      notes = "Return a single column specification in a specific table.",
      response = ColumnDefinition.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ColumnDefinition.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("/{columnName}")
  public Response getOneColumn(
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
      @ApiParam(value = "Name of the column to use for the request.", required = true)
          @PathParam("columnName")
          final String columnName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          restDB.authorizeSchemaRead(
              Collections.singletonList(keyspaceName),
              Collections.singletonList(tableName),
              SourceAPI.REST,
              ResourceKind.TABLE);

          final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);
          final Column col = tableMetadata.column(columnName);
          if (col == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(new ApiError(String.format("column '%s' not found in table", columnName)))
                .build();
          }

          //noinspection ConstantConditions
          String type = col.type() != null ? col.type().cqlDefinition() : null;
          return Response.status(Response.Status.OK)
              .entity(new ColumnDefinition(col.name(), type, col.kind() == Kind.Static))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a column", notes = "Delete a single column in a specific table.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("/{columnName}")
  public Response deleteColumn(
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
      @ApiParam(value = "Name of the column to use for the request.", required = true)
          @PathParam("columnName")
          final String columnName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.ALTER, SourceAPI.REST, ResourceKind.TABLE);

          restDB
              .queryBuilder()
              .alter()
              .table(keyspaceName, tableName)
              .dropColumn(columnName)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.NO_CONTENT).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @PUT
  @ApiOperation(
      value = "Update a column",
      notes = "Update a single column in a specific table.",
      response = SuccessResponse.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = SuccessResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("/{columnName}")
  public Response updateColumn(
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
      @PathParam("columnName") final String columnName,
      @ApiParam(value = "", required = true) @NotNull final ColumnUpdate columnUpdate,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          restDB.authorizeSchemaWrite(
              keyspaceName, tableName, Scope.ALTER, SourceAPI.REST, ResourceKind.TABLE);

          restDB
              .queryBuilder()
              .alter()
              .table(keyspaceName, tableName)
              .renameColumn(columnName, columnUpdate.getNewName())
              .build()
              .execute()
              .get();
          return Response.status(Response.Status.OK).entity(new SuccessResponse()).build();
        });
  }
}
