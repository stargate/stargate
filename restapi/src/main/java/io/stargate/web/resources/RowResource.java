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
package io.stargate.web.resources;

import com.codahale.metrics.annotation.Timed;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Table;
import io.stargate.web.models.Error;
import io.stargate.web.models.Filter;
import io.stargate.web.models.Query;
import io.stargate.web.models.RowAdd;
import io.stargate.web.models.RowResponse;
import io.stargate.web.models.RowUpdate;
import io.stargate.web.models.Rows;
import io.stargate.web.models.RowsResponse;
import io.stargate.web.models.SuccessResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@Path("/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows")
@Produces(MediaType.APPLICATION_JSON)
public class RowResource {

  private static final Logger logger = LoggerFactory.getLogger(RowResource.class);

  @Inject private Db db;

  private int DEFAULT_PAGE_SIZE = 100;

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve rows",
      notes = "Get rows from a table based on the primary key.",
      response = Rows.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Rows.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("/{primaryKey : (.+)?}")
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
                  "Value from the primary key column for the table. Define composite keys by separating values with semicolons (`val1;val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year;race_name`.",
              required = true)
          @PathParam("primaryKey")
          final PathSegment id,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final ResultSet r =
              db.getAuthnzService().executeDataReadWithAuthorization(
                  () ->
                      localDB
                          .query()
                          .select()
                          .from(keyspaceName, tableName)
                          .where(
                              buildWhereClause(
                                  localDB, keyspaceName, tableName, request.getRequestURI()))
                          .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                          .execute(),
                  token,
                  idFromPath(request.getRequestURI()),
                  db.getTable(localDB, keyspaceName, tableName));

          final List<Map<String, Object>> rows =
              r.rows().stream().map(Converters::row2Map).collect(Collectors.toList());

          return Response.status(Response.Status.OK)
              .entity(new RowResponse(rows.size(), rows))
              .build();
        });
  }

  private ResultSet executeQueryWithAuthorization(
      Callable<ResultSet> action, String token, List<String> primaryKeyValues, Table tableMetadata)
      throws Exception {
    // No need to check the query if we're not using JWT based auth
    if (!"AuthJwtService".equals(System.getProperty("stargate.auth_id"))) {
      return action.call();
    }

    // Grab the custom claims from the JWT. It's safe to work with the JWT as a plain Base64 encoded
    // json object here since by this point we've already authenticated the request.
    String[] parts = token.split("\\.");
    String decodedPayload = new String(Base64.getUrlDecoder().decode(parts[1]));
    JSONObject payload = new JSONObject(decodedPayload);
    JSONObject stargateClaims = payload.getJSONObject("stargate_claims");

    precheckQuery(stargateClaims, primaryKeyValues, tableMetadata);

    ResultSet result = action.call();

    postcheckQuery(stargateClaims, result);

    return result;
  }

  private void postcheckQuery(JSONObject stargateClaims, ResultSet resultSet)
      throws JSONException, UnauthorizedException {
    // If scopes/fields line up with the resultset then return
    if (resultSet != null) {
      for (Row row : resultSet.rows()) {
        for (Column col : row.columns()) {
          if (stargateClaims.has("x-stargate-" + col.name())) {
            // TODO: [doug] 2020-10-28, Wed, 16:47 needs to be waaaay safer
            if (!stargateClaims
                .getString("x-stargate-" + col.name())
                .equals(row.getString(col.name()))) {
              // TODO: [doug] 2020-10-28, Wed, 16:48 figure out how to return this as a 403
              throw new UnauthorizedException("Not allowed to access this resource");
            }
          }
        }
      }
    }
  }

  private void precheckQuery(JSONObject stargateClaims, List<String> primaryKeyValues,
      Table tableMetadata)
      throws JSONException, UnauthorizedException {
    List<Column> keys = tableMetadata.primaryKeyColumns();

    for (int i = 0; i < primaryKeyValues.size(); i++) {
      // If one of the columns exist as a field in the JWT claims and the values do not match then
      // the request is not allowed.
      if (stargateClaims.has("x-stargate-" + keys.get(i).name())) {

        if (!Column.ofTypeText(keys.get(i).type())) {
          throw new IllegalArgumentException(
              "Column must be of type text to be used for authorization");
        }

        // TODO: [doug] 2020-10-28, Wed, 16:47 needs to be waaaay safer
        if (!stargateClaims
            .getString("x-stargate-" + keys.get(i).name())
            .equals(primaryKeyValues.get(i))) {
          // TODO: [doug] 2020-10-28, Wed, 16:48 figure out how to return this as a 403
          throw new UnauthorizedException("Not allowed to access this resource");
        }
      }
    }
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve all rows",
      notes = "Get all rows from a table.",
      response = Rows.class)
  @ApiResponses(
      value = {
          @ApiResponse(code = 200, message = "OK", response = Rows.class),
          @ApiResponse(code = 400, message = "Bad request", response = Error.class),
          @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
          @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
          @ApiResponse(code = 404, message = "Not Found", response = Error.class),
          @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
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
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("pageSize")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("pageState")
          final String pageStateParam) {
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

          final ResultSet r =
              localDB
                  .query()
                  .select()
                  .from(keyspaceName, tableName)
                  .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                  .execute();

          final List<Map<String, Object>> rows =
              r.currentPageRows().stream().map(Converters::row2Map).collect(Collectors.toList());

          String newPagingState =
              r.getPagingState() != null
                  ? Base64.getEncoder().encodeToString(r.getPagingState().array())
                  : null;
          return Response.status(Response.Status.OK)
              .entity(new Rows(rows.size(), newPagingState, rows))
              .build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Submit queries",
      notes = "Submit queries to retrieve data from a table.",
      response = Rows.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Rows.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("/query")
  public Response queryRows(
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
      @ApiParam(value = "The query to be used for retrieving rows.", required = true) @NotNull
          final Query queryModel) {
    return RequestHandler.handle(
        () -> {
          ByteBuffer pageState = null;
          if (queryModel.getPageState() != null) {
            byte[] decodedBytes = Base64.getDecoder().decode(queryModel.getPageState());
            pageState = ByteBuffer.wrap(decodedBytes);
          }

          int pageSize = DEFAULT_PAGE_SIZE;
          if (queryModel.getPageSize() != null && queryModel.getPageSize() > 0) {
            pageSize = queryModel.getPageSize();
          }

          DataStore localDB = db.getDataStoreForToken(token, pageSize, pageState);

          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          String returnColumns = "*";
          if (queryModel.getColumnNames() != null && queryModel.getColumnNames().size() != 0) {
            returnColumns =
                queryModel.getColumnNames().stream()
                    .map(Converters::maybeQuote)
                    .collect(Collectors.joining(","));
          }

          if (queryModel.getFilters() == null || queryModel.getFilters().size() == 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new Error("filters must be provided"))
                .build();
          }

          List<Object> values = new ArrayList<>();
          for (Filter filter : queryModel.getFilters()) {
            if (!validateFilter(filter)) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(new Error("filter requires column name, operator, and value"))
                  .build();
            }

            for (Object obj : filter.getValue()) {
              values.add(filterToValue(obj, filter.getColumnName(), tableMetadata));
            }
          }
          String expression = buildExpressionFromOperators(queryModel.getFilters());

          String orderByExpression = "";
          if (queryModel.getOrderBy() != null) {
            String name = queryModel.getOrderBy().getColumn();
            String direction = queryModel.getOrderBy().getOrder();
            if (direction == null || name == null) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(new Error("both order and column are required for order by expression"))
                  .build();
            }

            direction = direction.toUpperCase();
            if (!direction.equals("ASC") && !direction.equals("DESC")) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(new Error("order must be either 'asc' or 'desc'"))
                  .build();
            }

            orderByExpression = "ORDER BY " + name + " " + direction;
          }

          String query =
              String.format(
                  "SELECT %s FROM %s.%s WHERE %s %s",
                  returnColumns, keyspaceName, tableName, expression, orderByExpression);
          CompletableFuture<ResultSet> selectQuery =
              localDB.query(query.trim(), ConsistencyLevel.LOCAL_QUORUM, values.toArray());

          ResultSet r = selectQuery.get();
          final List<Map<String, Object>> rows =
              r.currentPageRows().stream().map(Converters::row2Map).collect(Collectors.toList());

          String newPagingState =
              r.getPagingState() != null
                  ? Base64.getEncoder().encodeToString(r.getPagingState().array())
                  : null;
          return Response.status(Response.Status.OK)
              .entity(new Rows(rows.size(), newPagingState, rows))
              .build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Add row",
      notes =
          "Add a row to a table in your database. If the new row has the same primary key as that of an existing row, the database processes it as an update to the existing row.",
      response = RowsResponse.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = RowsResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  public Response addRow(
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
      @ApiParam(value = "Row object that needs to be added to the table", required = true) @NotNull
          final RowAdd rowAdd) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          List<Value<?>> values =
              rowAdd.getColumns().stream()
                  .map(
                      (c) ->
                          Converters.colToValue(
                              c.getName(),
                              c.getValue(),
                              db.getTable(localDB, keyspaceName, tableName)))
                  .collect(Collectors.toList());
          localDB
              .query()
              .insertInto(keyspaceName, tableName)
              .value(values)
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.CREATED).entity(new RowsResponse(true, 1)).build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete rows", notes = "Delete individual rows from a table.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("/{primaryKey}")
  public Response deleteRow(
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
                  "Value from the primary key column for the table. Define composite keys by separating values with semicolons (`val1;val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year;race_name`.",
              required = true)
          @PathParam("primaryKey")
          final PathSegment id,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          localDB
              .query()
              .delete()
              .from(keyspaceName, tableName)
              .where(buildWhereClause(localDB, keyspaceName, tableName, request.getRequestURI()))
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.NO_CONTENT).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @PUT
  @ApiOperation(
      value = "Update rows",
      notes = "Update existing rows in a table.",
      response = RowsResponse.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = RowsResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 403, message = "Forbidden", response = Error.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Error.class)
      })
  @Path("/{primaryKey}")
  public Response updateRow(
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
                  "Value from the primary key column for the table. Define composite keys by separating values with semicolons (`val1;val2...`) in the order they were defined. </br> For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)` then the primary key in the path would be `race_year;race_name`.",
              required = true)
          @PathParam("primaryKey")
          final PathSegment id,
      @Context HttpServletRequest request,
      final RowUpdate changeSet) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final Table tableMetadata = db.getTable(localDB, keyspaceName, tableName);

          List<Value<?>> changes =
              changeSet.getChangeset().stream()
                  .map((c) -> Converters.colToValue(c.getColumn(), c.getValue(), tableMetadata))
                  .collect(Collectors.toList());

          localDB
              .query()
              .update(keyspaceName, tableName)
              .value(changes)
              .where(buildWhereClause(request.getRequestURI(), tableMetadata))
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.OK).entity(new SuccessResponse()).build();
        });
  }

  private boolean validateFilter(Filter filter) {
    if (filter.getColumnName() == null) {
      return false;
    } else if (filter.getOperator() == null) {
      return false;
    } else if (filter.getValue() == null || filter.getValue().size() == 0) {
      return false;
    }

    return true;
  }

  private String buildExpressionFromOperators(List<Filter> filters) {
    StringBuilder expression = new StringBuilder();
    for (Filter filter : filters) {
      if (expression.length() != 0) {
        expression.append(" AND ");
      }

      String op = getOp(filter.getOperator());
      if (op.equals("in")) {
        String placeholder = String.join("", Collections.nCopies(filter.getValue().size(), "?,"));
        expression
            .append(filter.getColumnName())
            .append(" in (")
            .append(placeholder, 0, placeholder.length() - 1)
            .append(")");
      } else {
        expression.append(filter.getColumnName().toLowerCase()).append(" ").append(op).append(" ?");
      }
    }

    return expression.toString();
  }

  private String getOp(Filter.Operator operator) {
    switch (operator) {
      case eq:
        return "=";
      case notEq:
        return "!=";
      case gt:
        return ">";
      case gte:
        return ">=";
      case lt:
        return "<";
      case lte:
        return "<=";
      case in:
        return "in";
      default:
        return "=";
    }
  }

  private static Object filterToValue(Object val, String column, Table tableData) {
    Column.ColumnType type = tableData.column(column).type();
    Object value = val;

    if (type != null) {
      value = Converters.typeForStringValue(type, (String) val);
    }

    return value;
  }

  private List<Where<?>> buildWhereClause(
      DataStore localDB, String keyspaceName, String tableName, String path) {
    return buildWhereClause(path, db.getTable(localDB, keyspaceName, tableName));
  }

  private List<Where<?>> buildWhereClause(String path, Table tableMetadata) {
    List<String> values = idFromPath(path);

    final List<Column> keys = tableMetadata.primaryKeyColumns();
    boolean notAllPartitionKeys = values.size() < tableMetadata.partitionKeyColumns().size();
    boolean tooManyValues = values.size() > keys.size();
    if (tooManyValues || notAllPartitionKeys) {
      throw new IllegalArgumentException(
          String.format(
              "Number of key values provided (%s) should be in [%s, %s]. "
                  + "All partition key columns values are required plus 0..all clustering columns values in proper order.",
              values.size(), tableMetadata.partitionKeyColumns().size(), keys.size()));
    }

    return IntStream.range(0, values.size())
        .mapToObj(i -> Converters.idToWhere(values.get(i), keys.get(i).name(), tableMetadata))
        .collect(Collectors.toList());
  }

  private List<String> idFromPath(String path) {
    // Trim trailing / if it exists
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    String id = path.substring(path.lastIndexOf("/") + 1);
    List<String> ids = Arrays.asList(id.split(";"));

    for (int i = 0; i < ids.size(); i++) {
      try {
        ids.set(i, java.net.URLDecoder.decode(ids.get(i), StandardCharsets.UTF_8.name()));
      } catch (UnsupportedEncodingException e) {
        logger.warn("Unable to decode string", e);
        throw new RuntimeException(e);
      }
    }

    return ids;
  }
}
