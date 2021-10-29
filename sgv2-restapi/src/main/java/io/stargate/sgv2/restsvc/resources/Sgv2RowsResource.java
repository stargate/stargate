package io.stargate.sgv2.restsvc.resources;

import com.codahale.metrics.annotation.Timed;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2RowsResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
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
public class Sgv2RowsResource {
  private final int DEFAULT_PAGE_SIZE = 100;

  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /** Entity used to connect to backend gRPC service. */
  @Inject private GrpcClientFactory grpcFactory;

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
    if (isStringEmpty(fields)) {
      fields = "*";
    } else {
      fields = removeSpaces(fields);
    }
    final String cql = String.format("SELECT %s from %s.%s", fields, keyspaceName, tableName);
    logger.info("Calling gRPC method: try to call backend with CQL of '{}'", cql);

    StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    QueryOuterClass.QueryParameters.Builder paramsB = QueryOuterClass.QueryParameters.newBuilder();
    if (!isStringEmpty(pageStateParam)) {
      // surely there must better way to make Protobuf accept plain old byte[]? But if not:
      paramsB =
          paramsB.setPagingState(BytesValue.of(ByteString.copyFrom(decodeBase64(pageStateParam))));
    }

    QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql).build();
    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    logger.info("Calling gRPC method: response received {}", grpcResponse);

    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
    final int count = rs.getRowsCount();

    String pageStateStr = null;
    BytesValue pagingStateOut = rs.getPagingState();
    if (pagingStateOut.isInitialized()) {
      ByteString rawPS = pagingStateOut.getValue();
      if (!rawPS.isEmpty()) {
        byte[] b = rawPS.toByteArray();
        pageStateStr = Base64.getEncoder().encodeToString(b);
      }
    }

    List<QueryOuterClass.ColumnSpec> columns = rs.getColumnsList();
    for (QueryOuterClass.ColumnSpec column : columns) {
      //      QueryOuterClass.TypeSpec ctype = column.getType();
    }

    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      for (QueryOuterClass.Value value : row.getValuesList()) {
        QueryOuterClass.Collection c = value.getCollection();
        QueryOuterClass.Value.InnerCase cas = value.getInnerCase();
      }
    }

    Sgv2RowsResponse response = new Sgv2RowsResponse(count, pageStateStr, Collections.emptyList());
    return javax.ws.rs.core.Response.status(Response.Status.OK).entity(response).build();
  }

  private javax.ws.rs.core.Response handleGrpcDecodeError(
      String cql, InvalidProtocolBufferException e) {
    final String msg =
        String.format(
            "Problem decoding Protobuf content for CQL query '%s': %s", cql, e.getMessage());
    logger.error(msg, e);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(new RestServiceError(msg, Response.Status.BAD_REQUEST.getStatusCode()))
        .build();
  }

  // So we won't need Guava dependency; may be moved to our own util if needed elsewhere
  private static boolean isStringEmpty(String str) {
    return (str == null) || str.isEmpty();
  }

  private static String removeSpaces(String str) {
    // TODO: optimize/use commons-lang whatever
    return str.replaceAll("\\s", "");
  }

  private static byte[] decodeBase64(String base64encoded) {
    // TODO: error handling
    return Base64.getDecoder().decode(base64encoded);
  }
}
