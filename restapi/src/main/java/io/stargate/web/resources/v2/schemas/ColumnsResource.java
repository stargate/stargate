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
package io.stargate.web.resources.v2.schemas;

import com.codahale.metrics.annotation.Timed;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Table;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/schemas/keyspaces/{keyspaceName}/tables/{tableName}/columns")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnsResource {

  private static final Logger logger = LoggerFactory.getLogger(ColumnsResource.class);

  @Inject private Db db;

  @Timed
  @GET
  @ApiOperation(
      value = "List columns",
      notes = "Return all columns for a specified table.",
      response = ResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response getAll(
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
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final Table tableMetadata;
          try {
            tableMetadata = db.getTable(localDB, keyspaceName, tableName);
          } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("table '%s' not found", tableName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          List<ColumnDefinition> columnDefinitions =
              tableMetadata.columns().stream()
                  .map(
                      (col) -> {
                        String type = col.type() == null ? null : col.type().cqlDefinition();
                        return new ColumnDefinition(
                            col.name(), type, col.kind() == Column.Kind.Static);
                      })
                  .collect(Collectors.toList());

          Object response = raw ? columnDefinitions : new ResponseWrapper(columnDefinitions);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create a column",
      notes = "Add a single column to a table.",
      response = Map.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
      @ApiParam(value = "", required = true) @NotNull final ColumnDefinition columnDefinition) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          String name = columnDefinition.getName();
          Column.Kind kind = Column.Kind.Regular;
          if (columnDefinition.getIsStatic()) {
            kind = Column.Kind.Static;
          }

          Column column =
              ImmutableColumn.builder()
                  .name(name)
                  .kind(kind)
                  .type(Column.Type.fromCqlDefinitionOf(columnDefinition.getTypeDefinition()))
                  .build();

          localDB
              .query()
              .alter()
              .table(keyspaceName, tableName)
              .addColumn(column)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.CREATED)
              .entity(
                  Converters.writeResponse(
                      Collections.singletonMap("name", columnDefinition.getName())))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get a column",
      notes = "Return a single column specification in a specific table.",
      response = ColumnDefinition.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ColumnDefinition.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{columnName}")
  public Response getOne(
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
      @ApiParam(value = "column name", required = true) @PathParam("columnName")
          final String columnName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final Table tableMetadata;
          try {
            tableMetadata = db.getTable(localDB, keyspaceName, tableName);
          } catch (Exception e) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("table '%s' not found", tableName),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          final Column col = tableMetadata.column(columnName);
          if (col == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("column '%s' not found in table", columnName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          String type = col.type() == null ? null : col.type().cqlDefinition();
          ColumnDefinition columnDefinition =
              new ColumnDefinition(col.name(), type, col.kind() == Column.Kind.Static);
          Object response = raw ? columnDefinition : new ResponseWrapper(columnDefinition);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @PUT
  @ApiOperation(
      value = "Update a column",
      notes = "Update a single column in a specific table.",
      response = Map.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Map.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("/{columnName}")
  public Response update(
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
      @NotNull final ColumnDefinition columnUpdate) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          String alterInstructions =
              "RENAME "
                  + Converters.maybeQuote(columnName)
                  + " TO "
                  + Converters.maybeQuote(columnUpdate.getName());
          localDB
              .query(
                  String.format(
                      "ALTER TABLE %s.%s %s",
                      Converters.maybeQuote(keyspaceName),
                      Converters.maybeQuote(tableName),
                      alterInstructions),
                  Optional.of(ConsistencyLevel.LOCAL_QUORUM),
                  Collections.emptyList())
              .get();

          return Response.status(Response.Status.OK)
              .entity(
                  Converters.writeResponse(
                      Collections.singletonMap("name", columnUpdate.getName())))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a column", notes = "Delete a single column in a specific table.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{columnName}")
  public Response delete(
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
      @ApiParam(value = "column name", required = true) @PathParam("columnName")
          final String columnName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          localDB
              .query()
              .alter()
              .table(keyspaceName, tableName)
              .dropColumn(columnName)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }
}
