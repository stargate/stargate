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
import io.stargate.auth.Scope;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.web.models.ClusteringExpression;
import io.stargate.web.models.ColumnDefinition;
import io.stargate.web.models.Error;
import io.stargate.web.models.PrimaryKey;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.models.TableAdd;
import io.stargate.web.models.TableOptions;
import io.stargate.web.models.TableResponse;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/schemas/keyspaces/{keyspaceName}/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TablesResource {
  @Inject private Db db;

  @Timed
  @GET
  @ApiOperation(
      value = "Get all tables",
      notes = "Retrieve all tables in a specific keyspace.",
      response = ResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response getAllTables(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          List<TableResponse> tableResponses =
              db.getTables(localDB, keyspaceName).stream()
                  .map(this::getTable)
                  .collect(Collectors.toList());

          db.getAuthorizationService()
              .authorizeSchemaRead(
                  token,
                  Collections.singletonList(keyspaceName),
                  tableResponses.stream().map(TableResponse::getName).collect(Collectors.toList()));

          Object response = raw ? tableResponses : new ResponseWrapper(tableResponses);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get a table",
      notes = "Retrieve data for a single table in a specific keyspace.",
      response = Table.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Table.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);
          db.getAuthorizationService()
              .authorizeSchemaRead(
                  token,
                  Collections.singletonList(keyspaceName),
                  Collections.singletonList(tableName));

          Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          TableResponse tableResponse = getTable(tableMetadata);
          Object response = raw ? tableResponse : new ResponseWrapper(tableResponse);
          return Response.ok(Converters.writeResponse(response)).build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create a table",
      notes = "Add a table in a specific keyspace.",
      response = Map.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response createTable(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "", required = true) @NotNull final TableAdd tableAdd) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          Keyspace keyspace = localDB.schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "keyspace does not exists", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          String tableName = tableAdd.getName();
          if (tableName == null || tableName.equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "table name must be provided", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          PrimaryKey primaryKey = tableAdd.getPrimaryKey();
          if (primaryKey == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "primary key must be provided",
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          List<Column> columns = new ArrayList<>();
          TableOptions options = tableAdd.getTableOptions();
          for (ColumnDefinition colDef : tableAdd.getColumnDefinitions()) {
            String columnName = colDef.getName();
            if (columnName == null || columnName.equals("")) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(
                      new Error(
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
                      new Error(
                          "Unable to create table options " + e.getMessage(),
                          Response.Status.BAD_REQUEST.getStatusCode()))
                  .build();
            }
            columns.add(Column.create(columnName, kind, type, order));
          }

          db.getAuthorizationService()
              .authorizeSchemaWrite(token, keyspaceName, tableName, Scope.CREATE);
          localDB
              .queryBuilder()
              .create()
              .table(keyspaceName, tableName)
              .ifNotExists(tableAdd.getIfNotExists())
              .column(columns)
              .withDefaultTTL(options.getDefaultTimeToLive())
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.CREATED)
              .entity(Converters.writeResponse(Collections.singletonMap("name", tableName)))
              .build();
        });
  }

  @Timed
  @PUT
  @ApiOperation(
      value = "Replace a table definition",
      notes = "Update a single table definition, except for columns, in a keyspace.",
      response = Map.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "resource updated", response = Map.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{tableName}")
  public Response updateTable(
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
      @ApiParam(value = "table name", required = true) @NotNull final TableAdd tableUpdate) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          db.getAuthorizationService()
              .authorizeSchemaWrite(token, keyspaceName, tableName, Scope.ALTER);

          TableOptions options = tableUpdate.getTableOptions();
          List<ClusteringExpression> clusteringExpressions = options.getClusteringExpression();
          if (clusteringExpressions != null && !clusteringExpressions.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "Cannot update the clustering order of a table",
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          Integer defaultTTL = options.getDefaultTimeToLive();
          if (defaultTTL == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error("No update provided", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          localDB
              .queryBuilder()
              .alter()
              .table(keyspaceName, tableName)
              .withDefaultTTL(options.getDefaultTimeToLive())
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.CREATED)
              .entity(
                  Converters.writeResponse(Collections.singletonMap("name", tableUpdate.getName())))
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
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
          final String tableName) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          db.getAuthorizationService()
              .authorizeSchemaWrite(token, keyspaceName, tableName, Scope.DROP);

          localDB
              .queryBuilder()
              .drop()
              .table(keyspaceName, tableName)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  private TableResponse getTable(Table tableMetadata) {
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
        tableMetadata.partitionKeyColumns().stream().map(Column::name).collect(Collectors.toList());
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
    final int ttl =
        0; // TODO: [doug] 2020-09-1, Tue, 0:08 get this from schema (select default_time_to_live
    // from tables;)
    final TableOptions tableOptions = new TableOptions(ttl, clusteringExpression);

    return new TableResponse(
        tableMetadata.name(),
        tableMetadata.keyspace(),
        columnDefinitions,
        primaryKey,
        tableOptions);
  }
}
