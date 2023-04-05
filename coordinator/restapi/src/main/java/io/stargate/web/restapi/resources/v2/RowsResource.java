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
package io.stargate.web.restapi.resources.v2;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.codahale.metrics.annotation.Timed;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.core.util.ByteBufferUtils;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.ColumnOrder;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.web.models.ApiError;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.RequestHandler;
import io.stargate.web.restapi.dao.RestDB;
import io.stargate.web.restapi.dao.RestDBFactory;
import io.stargate.web.restapi.models.GetResponseWrapper;
import io.stargate.web.restapi.models.RESTResponseWrapper;
import io.stargate.web.restapi.resources.ResourceUtils;
import io.stargate.web.service.WhereParser;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class RowsResource {

  @Inject private RestDBFactory dbFactory;

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
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = ApiError.class)
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
                      + "| $eq | Equal To | \n "
                      + "| $ne | Not Equal To | \n "
                      + "| $in | Contained In | \n "
                      + "| $contains | Contains the given element (for lists or sets) or value (for maps) | \n "
                      + "| $containsKey | Contains the given key (for maps) | \n "
                      + "| $containsEntry | Contains the given key/value entry (for maps) | \n "
                      + "| $exists | Returns the rows whose column (boolean type) value is true | ",
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
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          if (Strings.isNullOrEmpty(where)) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new ApiError(
                        "where parameter is required", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          ByteBuffer pageState = null;
          if (pageStateParam != null) {
            pageState = ByteBufferUtils.fromBase64UrlParam(pageStateParam);
          }

          int pageSize = DEFAULT_PAGE_SIZE;
          if (pageSizeParam > 0) {
            pageSize = pageSizeParam;
          }

          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

          Object response =
              getRows(
                  fields,
                  raw,
                  sort,
                  restDB,
                  tableMetadata,
                  WhereParser.parseWhere(where, tableMetadata),
                  pageState,
                  pageSize);
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
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = ApiError.class)
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
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          ByteBuffer pageState = null;
          if (pageStateParam != null) {
            pageState = ByteBufferUtils.fromBase64UrlParam(pageStateParam);
          }

          int pageSize = DEFAULT_PAGE_SIZE;
          if (pageSizeParam > 0) {
            pageSize = pageSizeParam;
          }

          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

          List<BuiltCondition> where;
          try {
            where = buildWhereForPath(tableMetadata, path);
          } catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new ApiError(
                        "not enough partition keys provided: " + iae.getMessage(),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          Object response =
              getRows(fields, raw, sort, restDB, tableMetadata, where, pageState, pageSize);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve all rows",
      notes = "Get all rows from a table.",
      response = GetResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = GetResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = ApiError.class),
        @ApiResponse(code = 404, message = "Not Found", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = ApiError.class)
      })
  @Path("/rows")
  public Response getAllRows(
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
      @QueryParam("fields") final String fields,
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          ByteBuffer pageState = null;
          if (pageStateParam != null) {
            pageState = ByteBufferUtils.fromBase64UrlParam(pageStateParam);
          }

          int pageSize = DEFAULT_PAGE_SIZE;
          if (pageSizeParam > 0) {
            pageSize = pageSizeParam;
          }

          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));
          final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

          Object response =
              getRows(
                  fields,
                  raw,
                  sort,
                  restDB,
                  tableMetadata,
                  Collections.emptyList(),
                  pageState,
                  pageSize);
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
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 409, message = "Conflict", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = ApiError.class)
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
      @ApiParam(value = "", required = true) String payload,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          Map<String, Object> requestBody = ResourceUtils.readJson(payload);

          AbstractTable table = restDB.getTable(keyspaceName, tableName);

          List<ValueModifier> values =
              requestBody.entrySet().stream()
                  .map(e -> Converters.colToValue(e.getKey(), e.getValue(), table))
                  .collect(Collectors.toList());

          BoundQuery query =
              restDB
                  .queryBuilder()
                  .insertInto(keyspaceName, tableName)
                  .value(values)
                  .build()
                  .bind();

          restDB.authorizeDataWrite(
              keyspaceName,
              tableName,
              TypedKeyValue.forDML((BoundDMLQuery) query),
              Scope.MODIFY,
              SourceAPI.REST);

          restDB.execute(query, ConsistencyLevel.LOCAL_QUORUM).get();

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
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = ApiError.class)
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
      @ApiParam(value = "", required = true) String payload,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () ->
            modifyRow(token, keyspaceName, tableName, path, raw, payload, getAllHeaders(request)));
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete row(s)", notes = "Delete one or more rows in a table")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = ApiError.class)
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
          List<PathSegment> path,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

          List<BuiltCondition> where;
          try {
            where = buildWhereForPath(tableMetadata, path);
          } catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new ApiError(
                        "not enough partition keys provided: " + iae.getMessage(),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          BoundQuery query =
              restDB
                  .queryBuilder()
                  .delete()
                  .from(keyspaceName, tableName)
                  .where(where)
                  .build()
                  .bind();

          restDB.authorizeDataWrite(
              keyspaceName,
              tableName,
              TypedKeyValue.forDML((BoundDMLQuery) query),
              Scope.DELETE,
              SourceAPI.REST);

          restDB.execute(query, ConsistencyLevel.LOCAL_QUORUM).get();
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Timed
  @PATCH
  @ApiOperation(
      value = "Update part of a row(s)",
      notes = "Perform a partial update of one or more rows in a table",
      response = RESTResponseWrapper.class)
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 200,
            message = "resource updated",
            response = RESTResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = ApiError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = ApiError.class),
        @ApiResponse(code = 500, message = "Internal server error", response = ApiError.class)
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
      @ApiParam(value = "document", required = true) String payload,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () ->
            modifyRow(token, keyspaceName, tableName, path, raw, payload, getAllHeaders(request)));
  }

  private Response modifyRow(
      String token,
      String keyspaceName,
      String tableName,
      List<PathSegment> path,
      boolean raw,
      String payload,
      Map<String, String> headers)
      throws Exception {
    RestDB restDB = dbFactory.getRestDBForToken(token, headers);

    final AbstractTable tableMetadata = restDB.getTable(keyspaceName, tableName);

    List<BuiltCondition> where;
    try {
      where = buildWhereForPath(tableMetadata, path);
    } catch (IllegalArgumentException iae) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new ApiError(
                  "not enough partition keys provided: " + iae.getMessage(),
                  Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    }

    Map<String, Object> requestBody = ResourceUtils.readJson(payload);
    List<ValueModifier> changes =
        requestBody.entrySet().stream()
            .map(e -> Converters.colToValue(e.getKey(), e.getValue(), tableMetadata))
            .collect(Collectors.toList());

    BoundQuery query =
        restDB
            .queryBuilder()
            .update(keyspaceName, tableName)
            .value(changes)
            .where(where)
            .build()
            .bind();

    restDB.authorizeDataWrite(
        keyspaceName,
        tableName,
        TypedKeyValue.forDML((BoundDMLQuery) query),
        Scope.MODIFY,
        SourceAPI.REST);

    restDB.execute(query, ConsistencyLevel.LOCAL_QUORUM).get();
    Object response = raw ? requestBody : new RESTResponseWrapper(requestBody);
    return Response.status(Response.Status.OK).entity(Converters.writeResponse(response)).build();
  }

  private Object getRows(
      String fields,
      boolean raw,
      String sort,
      RestDB restDB,
      AbstractTable tableMetadata,
      List<BuiltCondition> where,
      ByteBuffer pageState,
      int pageSize)
      throws Exception {
    List<Column> columns;
    if (Strings.isNullOrEmpty(fields)) {
      columns = tableMetadata.columns();
    } else {
      columns =
          Arrays.stream(fields.split(","))
              .map(String::trim)
              .filter(c -> c.length() != 0)
              .map(Column::reference)
              .collect(Collectors.toList());
    }

    BoundQuery query =
        restDB
            .queryBuilder()
            .select()
            .column(columns)
            .from(tableMetadata.keyspace(), tableMetadata.name())
            .where(where)
            .orderBy(buildSortOrder(sort))
            .build()
            .bind();

    UnaryOperator<Parameters> parametersModifier =
        p -> {
          ImmutableParameters.Builder parametersBuilder =
              ImmutableParameters.builder().pageSize(pageSize);
          if (pageState != null) {
            parametersBuilder.pagingState(pageState);
          }
          return parametersBuilder.consistencyLevel(ConsistencyLevel.LOCAL_QUORUM).build();
        };

    final ResultSet r =
        restDB.authorizedDataRead(
            () -> restDB.execute(query, parametersModifier).get(),
            tableMetadata.keyspace(),
            tableMetadata.name(),
            TypedKeyValue.forSelect((BoundSelect) query),
            SourceAPI.REST);

    List<Map<String, Object>> rows =
        r.currentPageRows().stream().map(Converters::row2Map).collect(Collectors.toList());
    String newPagingState =
        r.getPagingState() != null ? ByteBufferUtils.toBase64ForUrl(r.getPagingState()) : null;
    return raw ? rows : new GetResponseWrapper(rows.size(), newPagingState, rows);
  }

  private List<ColumnOrder> buildSortOrder(String sort) {
    if (Strings.isNullOrEmpty(sort)) {
      return new ArrayList<>();
    }

    List<ColumnOrder> order = new ArrayList<>();
    Map<String, Object> sortOrder = ResourceUtils.readJson(sort);

    for (Map.Entry<String, Object> entry : sortOrder.entrySet()) {
      Column.Order colOrder =
          "asc".equalsIgnoreCase((String) entry.getValue()) ? Column.Order.ASC : Column.Order.DESC;
      order.add(ColumnOrder.of(entry.getKey(), colOrder));
    }
    return order;
  }

  private List<BuiltCondition> buildWhereForPath(
      AbstractTable tableMetadata, List<PathSegment> path) {
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
