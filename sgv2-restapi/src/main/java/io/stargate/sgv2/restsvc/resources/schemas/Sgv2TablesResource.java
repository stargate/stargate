package io.stargate.sgv2.restsvc.resources.schemas;

import com.codahale.metrics.annotation.Timed;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoTypeTranslator;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2ColumnDefinition;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.models.Sgv2Table;
import io.stargate.sgv2.restsvc.models.Sgv2TableAddRequest;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@ApiImplicitParams({
  @ApiImplicitParam(
      name = "X-Cassandra-Token",
      paramType = "header",
      value = "The token returned from the authorization endpoint. Use this token in each request.",
      required = true)
})
@Path("/v2/schemas/keyspaces/{keyspaceName}/tables")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2TablesResource extends ResourceBase {
  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Timed
  @GET
  @ApiOperation(
      value = "Get all tables",
      notes = "Retrieve all tables in a specific keyspace.",
      response = Sgv2Table.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Table.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  public Response getAllTables(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    List<Schema.CqlTable> tableDefs =
        BridgeSchemaClient.create(blockingStub).findAllTables(keyspaceName);
    List<Sgv2Table> tableResponses =
        tableDefs.stream().map(t -> table2table(t, keyspaceName)).collect(Collectors.toList());
    final Object payload = raw ? tableResponses : new Sgv2RESTResponse(tableResponses);
    return jaxrsResponse(Status.OK).entity(payload).build();
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get a table",
      notes = "Retrieve data for a single table in a specific keyspace.",
      response = Sgv2Table.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Table.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{tableName}")
  public Response getOneTable(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("table name must be provided", Status.BAD_REQUEST);
    }
    // NOTE: Can Not use "callWithTable()" as that would return 400 (Bad Request) for
    // missing Table; here we specifically want 404 instead.
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    Sgv2Table tableResponse = table2table(tableDef, keyspaceName);
    final Object payload = raw ? tableResponse : new Sgv2RESTResponse(tableResponse);
    return jaxrsResponse(Status.OK).entity(payload).build();
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
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 409, message = "Conflict", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  public Response createTable(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(required = true) @NotNull final Sgv2TableAddRequest tableAdd,
      @Context HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    final String tableName = tableAdd.getName();
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("table name must be provided", Status.BAD_REQUEST);
    }

    Schema.CqlTableCreate addTable =
        Schema.CqlTableCreate.newBuilder()
            .setKeyspaceName(keyspaceName)
            .setTable(table2table(tableAdd))
            .setIfNotExists(tableAdd.getIfNotExists())
            .build();
    BridgeSchemaClient.create(blockingStub).createTable(addTable);
    return jaxrsResponse(Status.CREATED)
        .entity(Collections.singletonMap("name", tableName))
        .build();
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
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(code = 409, message = "Conflict", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{tableName}")
  public Response updateTable(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @ApiParam(value = "table name", required = true) @NotNull
          final Sgv2TableAddRequest tableUpdate,
      @Context HttpServletRequest request) {
    if (isStringEmpty(keyspaceName)) {
      throw new WebApplicationException("keyspaceName must be provided", Status.BAD_REQUEST);
    }
    if (isStringEmpty(tableName)) {
      throw new WebApplicationException("table name must be provided", Status.BAD_REQUEST);
    }
    return callWithTable(
        blockingStub,
        keyspaceName,
        tableName,
        (tableDef) -> {
          Sgv2Table.TableOptions options = tableUpdate.getTableOptions();
          List<?> clusteringExpressions = options.getClusteringExpression();
          if (clusteringExpressions != null && !clusteringExpressions.isEmpty()) {
            throw new WebApplicationException(
                "Cannot update the clustering order of a table", Status.BAD_REQUEST);
          }
          Integer defaultTTL = options.getDefaultTimeToLive();
          // 09-Dec-2021, tatu: Seems bit odd but this is the way SGv1/RESTv2 checks it,
          //    probably since this is the only thing that can actually be changed:
          if (defaultTTL == null) {
            throw new WebApplicationException(
                "No update provided for defaultTTL", Status.BAD_REQUEST);
          }
          String cql =
              new QueryBuilder()
                  .alter()
                  .table(keyspaceName, tableName)
                  .withDefaultTTL(options.getDefaultTimeToLive())
                  .build();
          blockingStub.executeQuery(
              QueryOuterClass.Query.newBuilder()
                  .setParameters(parametersForLocalQuorum())
                  .setCql(cql)
                  .build());
          return jaxrsResponse(Response.Status.OK)
              .entity(Collections.singletonMap("name", tableName))
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
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{tableName}")
  public Response deleteTable(
      @Context StargateGrpc.StargateBlockingStub blockingStub,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the table to use for the request.", required = true)
          @PathParam("tableName")
          final String tableName,
      @Context HttpServletRequest request) {
    String cql = new QueryBuilder().drop().table(keyspaceName, tableName).ifExists().build();
    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    return jaxrsResponse(Status.NO_CONTENT).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for structural conversions
  /////////////////////////////////////////////////////////////////////////
   */

  private Schema.CqlTable table2table(Sgv2TableAddRequest restTable) {
    final String name = restTable.getName();
    Schema.CqlTable.Builder b = Schema.CqlTable.newBuilder().setName(name);

    // First, convert and add column definitions:
    final Sgv2Table.PrimaryKey primaryKeys = restTable.getPrimaryKey();
    for (Sgv2ColumnDefinition columnDef : restTable.getColumnDefinitions()) {
      final String columnName = columnDef.getName();
      QueryOuterClass.ColumnSpec column =
          QueryOuterClass.ColumnSpec.newBuilder()
              .setName(columnName)
              .setType(
                  BridgeProtoTypeTranslator.cqlTypeFromBridgeTypeSpec(
                      columnDef.getTypeDefinition()))
              .build();
      if (primaryKeys.hasPartitionKey(columnName)) {
        b.addPartitionKeyColumns(column);
      } else if (primaryKeys.hasClusteringKey(columnName)) {
        b.addClusteringKeyColumns(column);
      } else if (columnDef.getIsStatic()) {
        b.addStaticColumns(column);
      } else {
        b.addColumns(column);
      }
    }

    // And then add ordering (ASC, DESC)
    for (Sgv2Table.ClusteringExpression expr : restTable.findClusteringExpressions()) {
      if (expr.hasOrderAsc()) {
        b.putClusteringOrders(expr.getColumn(), Schema.ColumnOrderBy.ASC);
      } else if (expr.hasOrderDesc()) {
        b.putClusteringOrders(expr.getColumn(), Schema.ColumnOrderBy.DESC);
      } else {
        throw new IllegalArgumentException("Unrecognized ordering value '" + expr.getOrder() + "'");
      }
    }

    return b.build();
  }

  private Sgv2Table table2table(Schema.CqlTable grpcTable, String keyspace) {
    final List<Sgv2ColumnDefinition> columns = new ArrayList<>();
    final Sgv2Table.PrimaryKey primaryKeys = new Sgv2Table.PrimaryKey();

    // Not very pretty but need to both add columns AND create PrimaryKey defs so:
    for (QueryOuterClass.ColumnSpec column : grpcTable.getPartitionKeyColumnsList()) {
      columns.add(column2column(column, false));
      primaryKeys.addPartitionKey(column.getName());
    }
    for (QueryOuterClass.ColumnSpec column : grpcTable.getClusteringKeyColumnsList()) {
      columns.add(column2column(column, false));
      primaryKeys.addClusteringKey(column.getName());
    }
    for (QueryOuterClass.ColumnSpec column : grpcTable.getStaticColumnsList()) {
      columns.add(column2column(column, true));
    }
    for (QueryOuterClass.ColumnSpec column : grpcTable.getColumnsList()) {
      columns.add(column2column(column, false));
    }
    // !!! TODO: figure out where to find TTL?
    List<Sgv2Table.ClusteringExpression> clustering =
        clustering2clustering(grpcTable.getClusteringOrdersMap());
    final Sgv2Table.TableOptions tableOptions = new Sgv2Table.TableOptions(null, clustering);
    return new Sgv2Table(grpcTable.getName(), keyspace, columns, primaryKeys, tableOptions);
  }

  private Sgv2ColumnDefinition column2column(QueryOuterClass.ColumnSpec column, boolean isStatic) {
    return new Sgv2ColumnDefinition(
        column.getName(),
        BridgeProtoTypeTranslator.cqlTypeFromBridgeTypeSpec(column.getType()),
        isStatic);
  }

  private List<Sgv2Table.ClusteringExpression> clustering2clustering(
      Map<String, Schema.ColumnOrderBy> orders) {
    final List<Sgv2Table.ClusteringExpression> result = new ArrayList<>();
    orders.forEach(
        (k, v) -> {
          switch (v) {
            case ASC:
              result.add(
                  new Sgv2Table.ClusteringExpression(k, Sgv2Table.ClusteringExpression.VALUE_ASC));
              break;
            case DESC:
              result.add(
                  new Sgv2Table.ClusteringExpression(k, Sgv2Table.ClusteringExpression.VALUE_DESC));
              break;
          }
        });
    return result;
  }
}
