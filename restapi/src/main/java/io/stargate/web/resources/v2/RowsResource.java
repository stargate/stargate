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
package io.stargate.web.resources.v2;

import com.codahale.metrics.annotation.Timed;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.query.ColumnOrder;
import io.stargate.db.datastore.query.ImmutableColumnOrder;
import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.web.models.Error;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.stargate.web.service.WhereParser;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
public class RowsResource {

  private static final Logger logger = LoggerFactory.getLogger(RowsResource.class);

  @Inject private Db db;
  private static final ObjectMapper mapper = new ObjectMapper();
  private final int DEFAULT_PAGE_SIZE = 100;

  @Timed
  @GET
  @ApiOperation(
      value = "Search a table",
      notes = "Search a table using a json query as defined in the `where` query parameter",
      response = GetResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = GetResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response getRowWithWhere(
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
      @ApiParam(
              value =
                  "URL escaped JSON query using the following keys: \n "
                      + "| Key | Operation | \n "
                      + "|-|-| \n "
                      + "| $lt | Less Than | \n "
                      + "| $lte | Less Than Or Equal To | \n "
                      + "| $gt | Greater Than | \n "
                      + "| $gte | Greater Than Or Equal To | \n "
                      + "| $ne | Not Equal To | \n "
                      + "| $in | Contained In | \n "
                      + "| $contains | Contains the given element (for lists or sets) or value (for maps) | \n "
                      + "| $containsKey | Contains the given key (for maps) | \n "
                      + "| $containsEntry | Contains the given key/value entry (for maps) | \n "
                      + "| $exists | A value is set for the key | ",
              required = true)
          @QueryParam("where")
          final String where,
      @ApiParam(value = "URL escaped, comma delimited list of keys to include")
          @QueryParam("fields")
          final String fields,
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort) {
    return RequestHandler.handle(
        () -> {
          if (Strings.isNullOrEmpty(where)) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "where parameter is required", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          ByteBuffer pageState = null;
          if (pageStateParam != null) {
            byte[] decodedBytes = Base64.getDecoder().decode(pageStateParam);
            pageState = ByteBuffer.wrap(decodedBytes);
          }

          int pageSize = DEFAULT_PAGE_SIZE;
          if (pageSizeParam > 0) {
            pageSize = pageSizeParam;
          }

          DataStore localDB = db.getDataStoreForToken(token, pageSize, pageState);
          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          Object response =
              getRows(
                  fields,
                  raw,
                  sort,
                  localDB,
                  tableMetadata,
                  WhereParser.parseWhere(where, tableMetadata));
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get row(s)",
      notes = "Get rows from a table based on the primary key.",
      response = GetResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = GetResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{primaryKey: .*}")
  public Response getRows(
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
      @ApiParam(
              value =
                  "Value from the primary key column for the table. Define composite keys by separating values with slashes (`val1/val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @ApiParam(value = "URL escaped, comma delimited list of keys to include")
          @QueryParam("fields")
          final String fields,
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort) {
    return RequestHandler.handle(
        () -> {
          ByteBuffer pageState = null;
          if (pageStateParam != null) {
            byte[] decodedBytes = Base64.getDecoder().decode(pageStateParam);
            pageState = ByteBuffer.wrap(decodedBytes);
          }

          int pageSize = DEFAULT_PAGE_SIZE;
          if (pageSizeParam > 0) {
            pageSize = pageSizeParam;
          }

          DataStore localDB = db.getDataStoreForToken(token, pageSize, pageState);
          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          List<Where<?>> where;
          try {
            where = buildWhereForPath(tableMetadata, path);
          } catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "not enough partition keys provided",
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          Object response = getRows(fields, raw, sort, localDB, tableMetadata, where);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Add row",
      notes =
          "Add a row to a table in your database. If the new row has the same primary key as that of an existing row, the database processes it as an update to the existing row.",
      response = String.class,
      responseContainer = "Map",
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 201,
            message = "resource created",
            response = Map.class,
            responseContainer = "Map"),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response createRow(
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
      @ApiParam(value = "", required = true) String payload) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          Map<String, String> requestBody = mapper.readValue(payload, Map.class);

          Table table = db.getTable(localDB, keyspaceName, tableName);

          List<Value<?>> values =
              requestBody.entrySet().stream()
                  .map((e) -> Converters.colToValue(e, table))
                  .collect(Collectors.toList());

          localDB
              .query()
              .insertInto(keyspaceName, tableName)
              .value(values)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          Map<String, Object> keys = new HashMap<>();
          for (Column col : table.primaryKeyColumns()) {
            keys.put(col.name(), requestBody.get(col.name()));
          }

          return Response.status(Response.Status.CREATED)
              .entity(Converters.writeResponse(keys))
              .build();
        });
  }

  @Timed
  @PUT
  @ApiOperation(
      value = "Replace row(s)",
      notes = "Update existing rows in a table.",
      response = Object.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "resource updated", response = Object.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{primaryKey: .*}")
  public Response updateRows(
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
      @ApiParam(
              value =
                  "Value from the primary key column for the table. Define composite keys by separating values with slashes (`val1/val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(value = "", required = true) String payload) {
    return RequestHandler.handle(
        () -> modifyRow(token, keyspaceName, tableName, path, raw, payload));
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete row(s)", notes = "Delete one or more rows in a table")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{primaryKey: .*}")
  public Response deleteRows(
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
      @ApiParam(
              value =
                  "Value from the primary key column for the table. Define composite keys by separating values with slashes (`val1/val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          List<Where<?>> where;
          try {
            where = buildWhereForPath(tableMetadata, path);
          } catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "not enough partition keys provided",
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          localDB
              .query()
              .delete()
              .from(keyspaceName, tableName)
              .where(where)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Timed
  @PATCH
  @ApiOperation(
      value = "Update part of a row(s)",
      notes = "Perform a partial update of one or more rows in a table",
      response = ResponseWrapper.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "resource updated", response = ResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{primaryKey: .*}")
  public Response patchRows(
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
      @ApiParam(
              value =
                  "Value from the primary key column for the table. Define composite keys by separating values with slashes (`val1/val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year/race_name` ",
              required = true)
          @PathParam("primaryKey")
          List<PathSegment> path,
      @QueryParam("raw") final boolean raw,
      @ApiParam(value = "document", required = true) String payload) {
    return RequestHandler.handle(
        () -> modifyRow(token, keyspaceName, tableName, path, raw, payload));
  }

  private Response modifyRow(
      String token,
      String keyspaceName,
      String tableName,
      List<PathSegment> path,
      boolean raw,
      String payload)
      throws UnauthorizedException, com.fasterxml.jackson.core.JsonProcessingException,
          ExecutionException, InterruptedException {
    DataStore localDB = db.getDataStoreForToken(token);

    final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

    List<Where<?>> where;
    try {
      where = buildWhereForPath(tableMetadata, path);
    } catch (IllegalArgumentException iae) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new Error(
                  "not enough partition keys provided",
                  Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    }

    Map<String, String> requestBody = mapper.readValue(payload, Map.class);
    List<Value<?>> changes =
        requestBody.entrySet().stream()
            .map((e) -> Converters.colToValue(e, tableMetadata))
            .collect(Collectors.toList());

    final ResultSet r =
        localDB
            .query()
            .update(keyspaceName, tableName)
            .value(changes)
            .where(where)
            .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .execute();

    Object response = raw ? requestBody : new ResponseWrapper(requestBody);
    return Response.status(Response.Status.OK).entity(Converters.writeResponse(response)).build();
  }

  private Object getRows(
      String fields,
      boolean raw,
      String sort,
      DataStore localDB,
      Table tableMetadata,
      List<Where<?>> where)
      throws Exception {
    List<Column> columns;
    if (Strings.isNullOrEmpty(fields)) {
      columns = tableMetadata.columns();
    } else {
      columns =
          Arrays.stream(fields.split(",")).map(Column::reference).collect(Collectors.toList());
    }

    final ResultSet r =
        localDB
            .query()
            .select()
            .column(columns)
            .from(tableMetadata.keyspace(), tableMetadata.name())
            .where(where)
            .orderBy(buildSortOrder(sort))
            .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
            .execute();

    List<Map<String, Object>> rows =
        r.currentPageRows().stream().map(Converters::row2Map).collect(Collectors.toList());
    String newPagingState =
        r.getPagingState() != null
            ? Base64.getEncoder().encodeToString(r.getPagingState().array())
            : null;
    return raw ? rows : new GetResponseWrapper(rows.size(), newPagingState, rows);
  }

  private List<ColumnOrder> buildSortOrder(String sort)
      throws com.fasterxml.jackson.core.JsonProcessingException {
    if (Strings.isNullOrEmpty(sort)) {
      return new ArrayList<>();
    }

    List<ColumnOrder> order = new ArrayList<>();
    Map<String, String> sortOrder = mapper.readValue(sort, Map.class);

    for (Map.Entry<String, String> entry : sortOrder.entrySet()) {
      Column.Order colOrder =
          "asc".equals(entry.getValue().toLowerCase()) ? Column.Order.Asc : Column.Order.Desc;
      order.add(ImmutableColumnOrder.of(entry.getKey(), colOrder));
    }
    return order;
  }

  private List<Where<?>> buildWhereForPath(Table tableMetadata, List<PathSegment> path) {
    List<Column> keys = tableMetadata.primaryKeyColumns();
    boolean notAllPartitionKeys = path.size() < tableMetadata.partitionKeyColumns().size();
    boolean tooManyValues = path.size() > keys.size();
    if (tooManyValues || notAllPartitionKeys) {
      throw new IllegalArgumentException(
          String.format(
              "Number of key values provided (%s) should be in [%s, %s]. "
                  + "All partition key columns values are required plus 0..all clustering columns values in proper order.",
              path.size(), tableMetadata.partitionKeyColumns().size(), keys.size()));
    }

    return IntStream.range(0, path.size())
        .mapToObj(
            i -> Converters.idToWhere(path.get(i).getPath(), keys.get(i).name(), tableMetadata))
        .collect(Collectors.toList());
  }
}
