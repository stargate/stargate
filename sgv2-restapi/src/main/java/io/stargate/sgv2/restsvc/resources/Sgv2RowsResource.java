package io.stargate.sgv2.restsvc.resources;

import com.codahale.metrics.annotation.Timed;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Int32Value;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.ExtProtoValueConverter;
import io.stargate.sgv2.restsvc.grpc.ExtProtoValueConverters;
import io.stargate.sgv2.restsvc.grpc.JsonToProtoValueConverter;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2GetResponse;
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
      response = Sgv2GetResponse.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2GetResponse.class),
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
    // 10-Nov-2021, tatu: Can not implement quite yet due to lack of schema access to bind
    //     "primaryKey" segments to actual columns.
    return Sgv2RequestHandler.handle(
        () -> {
          return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).build();
        });
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

    StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    QueryOuterClass.QueryParameters.Builder paramsB = QueryOuterClass.QueryParameters.newBuilder();
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

    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql).build();
    return Sgv2RequestHandler.handle(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

          final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
          final int count = rs.getRowsCount();

          String pageStateStr = extractPagingStateFromResultSet(rs);
          Sgv2RowsResponse response = new Sgv2RowsResponse(count, pageStateStr, convertRows(rs));
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
    QueryOuterClass.Values.Builder valuesBuilder = QueryOuterClass.Values.newBuilder();
    final String cql;

    try {
      cql = buildAddRowCQL(keyspaceName, tableName, payloadMap, valuesBuilder);
    } catch (Exception e) {
      return jaxrsServiceError(
              Response.Status.INTERNAL_SERVER_ERROR, "Failure to bind payload " + e.getMessage())
          .build();
    }
    logger.info("createRow(): try to call backend with CQL of '{}'", cql);

    StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    QueryOuterClass.QueryParameters params =
        QueryOuterClass.QueryParameters.newBuilder()
            .setConsistency(
                QueryOuterClass.ConsistencyValue.newBuilder()
                    .setValue(QueryOuterClass.Consistency.LOCAL_QUORUM))
            .build();
    final QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder()
            .setParameters(params)
            .setCql(cql)
            .setValues(valuesBuilder.build())
            .build();

    return Sgv2RequestHandler.handle(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);
          // apparently no useful data in ResultSet, we should simply return payload we got:
          return jaxrsResponse(Response.Status.CREATED).entity(payloadAsString).build();
        });
  }

  // // // Helper methods: Query construction

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
      QueryOuterClass.Values.Builder valuesBuilder) {
    OngoingValues insert = QueryBuilder.insertInto(keyspaceName, tableName);
    for (Map.Entry<String, Object> entry : payloadMap.entrySet()) {
      insert = insert.value(entry.getKey(), QueryBuilder.bindMarker());
      valuesBuilder.addValues(JsonToProtoValueConverter.protoValueFor(entry.getValue()));
    }
    return ((Insert) insert).asCql();
  }

  // // // Helper methods: Structural/nested conversions

  private List<Map<String, Object>> convertRows(QueryOuterClass.ResultSet rs) {
    ExtProtoValueConverter converter =
        ExtProtoValueConverters.instance().createConverter(rs.getColumnsList());
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
}
