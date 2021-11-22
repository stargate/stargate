package io.stargate.sgv2.restsvc.resources;

import com.codahale.metrics.annotation.Timed;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoValueConverters;
import io.stargate.sgv2.restsvc.grpc.BridgeSchemaClient;
import io.stargate.sgv2.restsvc.grpc.FromProtoConverter;
import io.stargate.sgv2.restsvc.grpc.ToProtoConverter;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2RowsResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"data"})
@Path("/v2/keyspaces/{keyspaceName}/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Sgv2RowsResource extends ResourceBase {
  private static final int DEFAULT_PAGE_SIZE = 100;

  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Entity used to connect to backend gRPC service. */
  @Inject private GrpcClientFactory grpcFactory;

  @Timed
  @GET
  @ApiOperation(
      value = "Get row(s)",
      notes = "Get rows from a table based on the primary key.",
      response = Sgv2RowsResponse.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2RowsResponse.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
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
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    List<String> columns = isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));

    // To bind path/key parameters, need converter; and for that we need table metadata:
    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    final String cql =
        buildGetRowsByPKCQL(
            keyspaceName, tableName, path, columns, tableDef, valuesBuilder, toProtoConverter);
    logger.info("getRows(): try to call backend with CQL of '{}'", cql);

    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, valuesBuilder);
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Retrieve all rows",
      notes = "Get all rows from a table.",
      response = Sgv2RowsResponse.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2RowsResponse.class),
        @ApiResponse(code = 400, message = "Bad request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 403, message = "Forbidden", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal Server Error",
            response = RestServiceError.class)
      })
  @Path("/rows")
  public javax.ws.rs.core.Response getAllRows(
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
      @QueryParam("fields") String fields,
      @ApiParam(value = "Restrict the number of returned items") @QueryParam("page-size")
          final int pageSizeParam,
      @ApiParam(value = "Move the cursor to a particular result") @QueryParam("page-state")
          final String pageStateParam,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @ApiParam(value = "Keys to sort by") @QueryParam("sort") final String sort,
      @Context HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    List<String> columns = isStringEmpty(fields) ? Collections.emptyList() : splitColumns(fields);
    final String cql = buildGetAllRowsCQL(keyspaceName, tableName, columns);
    logger.info("getAllRows(): try to call backend with CQL of '{}'", cql);

    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    return fetchRows(blockingStub, pageSizeParam, pageStateParam, raw, cql, null);
  }

  private javax.ws.rs.core.Response fetchRows(
      StargateGrpc.StargateBlockingStub blockingStub,
      int pageSizeParam,
      String pageStateParam,
      boolean raw,
      String cql,
      QueryOuterClass.Values.Builder values) {
    QueryOuterClass.QueryParameters.Builder paramsB = parametersBuilderForLocalQuorum();
    if (!isStringEmpty(pageStateParam)) {
      // surely there must better way to make Protobuf accept plain old byte[]? But if not:
      paramsB =
          paramsB.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pageStateParam))));
    }
    int pageSize = DEFAULT_PAGE_SIZE;
    if (pageSizeParam > 0) {
      pageSize = pageSizeParam;
    }
    paramsB = paramsB.setPageSize(Int32Value.of(pageSize));

    QueryOuterClass.Query.Builder b =
        QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql);
    if (values != null) {
      b = b.setValues(values);
    }
    final QueryOuterClass.Query query = b.build();
    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

          final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
          final int count = rs.getRowsCount();

          String pageStateStr = extractPagingStateFromResultSet(rs);
          List<Map<String, Object>> rows = convertRows(rs);
          Object response = raw ? rows : new Sgv2RowsResponse(count, pageStateStr, rows);
          return jaxrsResponse(Response.Status.OK).entity(response).build();
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
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 409, message = "Conflict", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
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
      @ApiParam(value = "", required = true) final String payloadAsString,
      @Context HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    Map<String, Object> payloadMap;
    try {
      payloadMap = parseJsonAsMap(payloadAsString);
    } catch (Exception e) {
      return jaxrsServiceError(
              Response.Status.BAD_REQUEST, "Invalid JSON payload: " + e.getMessage())
          .build();
    }
    final String cql;

    final StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));

    Schema.CqlTable tableDef =
        BridgeSchemaClient.create(blockingStub).findTable(keyspaceName, tableName);
    final ToProtoConverter toProtoConverter = findProtoConverter(tableDef);
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();

    try {
      cql = buildAddRowCQL(keyspaceName, tableName, payloadMap, valuesBuilder, toProtoConverter);
    } catch (IllegalArgumentException e) {
      // No logging since this is due to something bad client sent:
      return jaxrsBadRequestError(e.getMessage()).build();
    } catch (Exception e) {
      // Unrecognized problem to be converted to something known; log:
      logger.error("Unknown payload bind problem: " + e.getMessage(), e);
      return jaxrsServiceError(
              Response.Status.INTERNAL_SERVER_ERROR, "Failure to bind payload: " + e.getMessage())
          .build();
    }
    logger.info("createRow(): try to call backend with CQL of '{}'", cql);

    QueryOuterClass.QueryParameters params = parametersForLocalQuorum();
    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(params)
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    return Sgv2RequestHandler.handleMainOperation(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);
          // apparently no useful data in ResultSet, we should simply return payload we got:
          return jaxrsResponse(Response.Status.CREATED).entity(payloadAsString).build();
        });
  }

  // // // Helper methods: Query construction

  private String buildGetRowsByPKCQL(
      String keyspaceName,
      String tableName,
      List<PathSegment> pkValues,
      List<String> columns,
      Schema.CqlTable tableDef,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    SelectFrom selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName);
    Select select;
    if (columns.isEmpty()) {
      select = selectFrom.all();
    } else {
      select = selectFrom.columns(columns);
    }
    final int keysIncluded = pkValues.size();
    List<QueryOuterClass.ColumnSpec> partitionKeys = tableDef.getPartitionKeyColumnsList();

    // Check we have "just right" number of keys
    if (keysIncluded < partitionKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Number of key values provided (%d) less than number of partition keys (%d)",
              keysIncluded, partitionKeys.size()));
    }
    List<QueryOuterClass.ColumnSpec> clusteringKeys = tableDef.getClusteringKeyColumnsList();
    List<QueryOuterClass.ColumnSpec> primaryKeys = concat(partitionKeys, clusteringKeys);
    if (keysIncluded > primaryKeys.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Number of key values provided (%d) exceeds number of partition keys (%d) and clustering keys (%d)",
              keysIncluded, partitionKeys.size(), clusteringKeys.size()));
    }

    for (int i = 0; i < keysIncluded; ++i) {
      final String keyValue = pkValues.get(i).getPath();
      QueryOuterClass.ColumnSpec column = primaryKeys.get(i);
      final String fieldName = column.getName();
      select = select.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker());
      valuesBuilder.addValues(toProtoConverter.protoValueFromStringified(fieldName, keyValue));
    }

    return select.asCql();
  }

  private String buildGetAllRowsCQL(String keyspaceName, String tableName, List<String> columns) {
    // return String.format("SELECT %s from %s.%s", fields, keyspaceName, tableName);
    SelectFrom selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName);
    if (columns.isEmpty()) {
      return selectFrom.all().asCql();
    }
    return selectFrom.columns(columns).asCql();
  }

  private String buildAddRowCQL(
      String keyspaceName,
      String tableName,
      Map<String, Object> payloadMap,
      QueryOuterClass.Values.Builder valuesBuilder,
      ToProtoConverter toProtoConverter) {
    OngoingValues insert = QueryBuilder.insertInto(keyspaceName, tableName);
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      final String fieldName = entry.getKey();
      insert = insert.value(fieldName, QueryBuilder.bindMarker());
      valuesBuilder.addValues(
          toProtoConverter.protoValueFromLooselyTyped(fieldName, entry.getValue()));
    }
    return ((Insert) insert).asCql();
  }

  // // // Helper methods: Structural/nested conversions

  private List<Map<String, Object>> convertRows(QueryOuterClass.ResultSet rs) {
    FromProtoConverter converter =
        BridgeProtoValueConverters.instance().fromProtoConverter(rs.getColumnsList());
    List<Map<String, Object>> resultRows = new ArrayList<>();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.mapFromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }

  // // // Helper methods: Simple scalar conversions

  private static List<String> splitColumns(String columnStr) {
    return Arrays.stream(columnStr.split(","))
        .map(String::trim)
        .filter(c -> c.length() != 0)
        .collect(Collectors.toList());
  }

  private static byte[] decodeBase64(String base64encoded) {
    // TODO: error handling
    return Base64.getDecoder().decode(base64encoded);
  }

  private static <T> List<T> concat(List<T> a, List<T> b) {
    if (a.isEmpty()) {
      return b;
    }
    if (b.isEmpty()) {
      return a;
    }
    ArrayList<T> result = new ArrayList<>(a);
    result.addAll(b);
    return result;
  }
}
