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
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.query.Value;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Table;
import io.stargate.web.models.Error;
import io.stargate.web.models.Filter;
import io.stargate.web.models.Query;
import io.stargate.web.models.RowAdd;
import io.stargate.web.models.RowResponse;
import io.stargate.web.models.RowUpdate;
import io.stargate.web.models.Rows;
import io.stargate.web.models.RowsResponse;
import io.stargate.web.models.SuccessResponse;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/keyspaces/{keyspaceName}/tables/{tableName}/rows")
@Produces(MediaType.APPLICATION_JSON)
public class RowResource {
  private static final Logger logger = LoggerFactory.getLogger(RowResource.class);

  @Inject private Db db;

  private int DEFAULT_PAGE_SIZE = 100;

  @Timed
  @GET
  @Path("/{id}")
  public Response getOne(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @PathParam("id") final PathSegment id) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          final ResultSet r =
              localDB
                  .query()
                  .select()
                  .from(keyspaceName, tableName)
                  .where(buildWhereClause(localDB, keyspaceName, tableName, id))
                  .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
                  .execute();

          final List<Map<String, Object>> rows =
              r.rows().stream().map(Converters::row2Map).collect(Collectors.toList());

          return Response.status(Response.Status.OK)
              .entity(new RowResponse(rows.size(), rows))
              .build();
        });
  }

  @Timed
  @GET
  public Response getAll(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @QueryParam("pageSize") final int pageSizeParam,
      @QueryParam("pageState") final String pageStateParam) {
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
              r.rows().stream().map(Converters::row2Map).collect(Collectors.toList());

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
  @Path("/query")
  public Response query(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @NotNull final Query queryModel) {
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
          List<Object> values = new ArrayList<>();
          for (Filter filter : queryModel.getFilters()) {
            for (Object obj : filter.getValue()) {
              values.add(filterToValue(obj, filter.getColumnName().toLowerCase(), tableMetadata));
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
              localDB.query(
                  query.trim(), Optional.of(ConsistencyLevel.LOCAL_QUORUM), values.toArray());

          ResultSet r = selectQuery.get();
          final List<Map<String, Object>> rows =
              r.rows().stream().map(Converters::row2Map).collect(Collectors.toList());

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
  public Response addRow(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @NotNull final RowAdd rowAdd) {
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
  @Path("/{id}")
  public Response deleteRow(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @PathParam("id") final PathSegment id) {
    return RequestHandler.handle(
        () -> {
          DataStore localDB = db.getDataStoreForToken(token);

          localDB
              .query()
              .delete()
              .from(keyspaceName, tableName)
              .where(buildWhereClause(localDB, keyspaceName, tableName, id))
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.NO_CONTENT).entity(new SuccessResponse()).build();
        });
  }

  @Timed
  @PUT
  @Path("/{id}")
  public Response updateRow(
      @HeaderParam("X-Cassandra-Token") String token,
      @PathParam("keyspaceName") final String keyspaceName,
      @PathParam("tableName") final String tableName,
      @PathParam("id") final PathSegment id,
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
              .where(buildWhereClause(id, tableMetadata))
              .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
              .execute();

          return Response.status(Response.Status.OK).entity(new SuccessResponse()).build();
        });
  }

  private String buildExpressionFromOperators(List<Filter> filters) {
    StringBuilder expression = new StringBuilder();
    for (Filter filter : filters) {
      if (!expression.toString().equals("")) {
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
      DataStore localDB, String keyspaceName, String tableName, PathSegment id) {
    return buildWhereClause(id, db.getTable(localDB, keyspaceName, tableName));
  }

  private List<Where<?>> buildWhereClause(PathSegment id, Table tableMetadata) {
    List<String> values = idFromPath(id);

    final List<Column> keys = tableMetadata.partitionKeyColumns();
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException("not enough partition keys provided");
    }

    return IntStream.range(0, keys.size())
        .mapToObj(i -> Converters.idToWhere(values.get(i), keys.get(i).name(), tableMetadata))
        .collect(Collectors.toList());
  }

  private List<String> idFromPath(PathSegment id) {
    MultivaluedMap<String, String> matrixParameters = id.getMatrixParameters();
    List<String> values = new ArrayList<>(matrixParameters.keySet());
    values.add(0, id.getPath());
    return values;
  }
}
